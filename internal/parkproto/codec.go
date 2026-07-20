package parkproto

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type Reader struct {
	r       io.Reader
	version uint16
	maxSize uint32
	lastSeq uint64
}

type Writer struct {
	w       io.Writer
	version uint16
	nextSeq uint64
}

type Received struct {
	Sequence uint64
	Message  Message
}

type envelope struct {
	Version  uint16          `json:"version"`
	Sequence uint64          `json:"sequence"`
	Type     MessageType     `json:"type"`
	Payload  json.RawMessage `json:"payload"`
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: r, version: Version, maxSize: MaxFrameSize}
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w, version: Version}
}

func (reader *Reader) Read() (Received, error) {
	frame, err := readRawFrame(reader.r, reader.maxSize)
	if err != nil {
		return Received{}, err
	}
	var env envelope
	if err := strictUnmarshalJSON(frame, &env); err != nil {
		return Received{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if env.Version != reader.version {
		return Received{}, fmt.Errorf("%w: got %d want %d", ErrVersionMismatch, env.Version, reader.version)
	}
	if env.Sequence == 0 {
		return Received{}, fmt.Errorf("%w: sequence is required", ErrMalformed)
	}
	if env.Sequence != reader.lastSeq+1 {
		return Received{}, fmt.Errorf("%w: got %d want %d", ErrSequence, env.Sequence, reader.lastSeq+1)
	}
	if len(env.Payload) == 0 {
		return Received{}, fmt.Errorf("%w: payload is required", ErrMalformed)
	}
	message, err := decodePayload(env.Type, env.Payload)
	if err != nil {
		return Received{}, err
	}
	reader.lastSeq = env.Sequence
	return Received{Sequence: env.Sequence, Message: message}, nil
}

func (writer *Writer) Write(message Message) (uint64, error) {
	writer.nextSeq++
	if err := WriteFrame(writer.w, writer.version, writer.nextSeq, message); err != nil {
		writer.nextSeq--
		return 0, err
	}
	return writer.nextSeq, nil
}

func WriteFrame(w io.Writer, version uint16, sequence uint64, message Message) error {
	if message == nil {
		return fmt.Errorf("%w: message is required", ErrMalformed)
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	env := envelope{
		Version:  version,
		Sequence: sequence,
		Type:     message.messageType(),
		Payload:  payload,
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if len(raw) > MaxFrameSize {
		return fmt.Errorf("%w: encoded frame is %d bytes", ErrOversized, len(raw))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(raw)))
	if err := writeFull(w, header[:]); err != nil {
		return err
	}
	return writeFull(w, raw)
}

func (writer *Writer) WriteIdentityReport(report IdentityReport) (uint64, error) {
	if err := report.Validate(); err != nil {
		return 0, err
	}
	return writer.Write(report)
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (writer *Writer) WriteRelease(release Release) (uint64, error) {
	return writer.Write(release)
}

func (writer *Writer) WriteReleaseAck(ack ReleaseAck) (uint64, error) {
	if err := ack.Validate(); err != nil {
		return 0, err
	}
	return writer.Write(ack)
}

func readRawFrame(r io.Reader, maxSize uint32) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrTruncated, err)
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 {
		return nil, fmt.Errorf("%w: frame length is zero", ErrMalformed)
	}
	if size > maxSize {
		return nil, fmt.Errorf("%w: frame length %d exceeds %d", ErrOversized, size, maxSize)
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTruncated, err)
	}
	return raw, nil
}

func decodePayload(messageType MessageType, payload json.RawMessage) (Message, error) {
	switch messageType {
	case MessageIdentityReport:
		var report IdentityReport
		if err := strictUnmarshalJSON(payload, &report); err != nil {
			return nil, fmt.Errorf("%w: identity report: %v", ErrMalformed, err)
		}
		if err := report.Validate(); err != nil {
			return nil, fmt.Errorf("%w: identity report: %v", ErrMalformed, err)
		}
		return report, nil
	case MessageRelease:
		var release strictReleaseJSON
		if err := strictUnmarshalJSON(payload, &release); err != nil {
			return nil, fmt.Errorf("%w: release: %v", ErrMalformed, err)
		}
		decoded, err := release.toRelease()
		if err != nil {
			return nil, fmt.Errorf("%w: release: %v", ErrMalformed, err)
		}
		return decoded, nil
	case MessageReleaseAck:
		var ack ReleaseAck
		if err := strictUnmarshalJSON(payload, &ack); err != nil {
			return nil, fmt.Errorf("%w: release ack: %v", ErrMalformed, err)
		}
		if err := ack.Validate(); err != nil {
			return nil, fmt.Errorf("%w: release ack: %v", ErrMalformed, err)
		}
		return ack, nil
	default:
		return nil, fmt.Errorf("%w: unknown message type %q", ErrMalformed, messageType)
	}
}

func strictUnmarshalJSON(raw []byte, out any) error {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing struct{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		seenFolded := make(map[string]string)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			foldedKey := foldedJSONKey(key)
			if previous, exists := seenFolded[foldedKey]; exists {
				return fmt.Errorf("duplicate JSON key %q conflicts with %q by case-insensitive match", key, previous)
			}
			seenFolded[foldedKey] = key
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		return consumeJSONEnd(decoder, '}')
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
		return consumeJSONEnd(decoder, ']')
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
}

func foldedJSONKey(key string) string {
	folded := make([]byte, 0, len(key))
	for i := 0; i < len(key); {
		if c := key[i]; c < utf8.RuneSelf {
			if 'a' <= c && c <= 'z' {
				c -= 'a' - 'A'
			}
			folded = append(folded, c)
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(key[i:])
		folded = utf8.AppendRune(folded, foldJSONRune(r))
		i += size
	}
	return string(folded)
}

func foldJSONRune(r rune) rune {
	for {
		next := unicode.SimpleFold(r)
		if next <= r {
			return next
		}
		r = next
	}
}

func consumeJSONEnd(decoder *json.Decoder, want json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != want {
		return fmt.Errorf("unexpected JSON delimiter %q, want %q", token, want)
	}
	return nil
}

type strictReleaseJSON struct {
	Binding          ReleaseBinding     `json:"binding"`
	ExpectedGroupRef strictGroupRefJSON `json:"expectedGroupRef"`
	ExecSpec         ExecSpec           `json:"execSpec"`
}

func (release strictReleaseJSON) toRelease() (Release, error) {
	groupRef, err := release.ExpectedGroupRef.toGroupRef()
	if err != nil {
		return Release{}, err
	}
	return Release{
		Binding:          release.Binding,
		ExpectedGroupRef: groupRef,
		ExecSpec:         release.ExecSpec,
	}, nil
}

type strictGroupRefJSON struct {
	Version             *uint16
	CustodyID           *model.CustodyID
	Launch              *model.LaunchKey
	HostBootID          *string
	PIDNamespaceID      string `json:"PIDNamespaceID,omitempty"`
	PIDNamespaceState   *model.PIDNamespaceState
	RetainedDomainID    string `json:"RetainedDomainID,omitempty"`
	RetainedDomainState *model.RetainedDomainState
	PGID                *int
	Leader              *model.ProcessIdentity
	Monitor             *model.ProcessIdentity
	RetainedID          *string
}

func (ref strictGroupRefJSON) toGroupRef() (model.GroupRef, error) {
	if ref.Version == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.Version is required")
	}
	if ref.CustodyID == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.CustodyID is required")
	}
	if ref.Launch == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.Launch is required")
	}
	if ref.HostBootID == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.HostBootID is required")
	}
	if ref.PIDNamespaceState == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.PIDNamespaceState is required")
	}
	if ref.RetainedDomainState == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.RetainedDomainState is required")
	}
	if ref.PGID == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.PGID is required")
	}
	if ref.Leader == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.Leader is required")
	}
	if ref.Monitor == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.Monitor is required")
	}
	if ref.RetainedID == nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef.RetainedID is required")
	}
	groupRef := model.GroupRef{
		Version:             *ref.Version,
		CustodyID:           *ref.CustodyID,
		Launch:              *ref.Launch,
		HostBootID:          *ref.HostBootID,
		PIDNamespaceID:      ref.PIDNamespaceID,
		PIDNamespaceState:   *ref.PIDNamespaceState,
		RetainedDomainID:    ref.RetainedDomainID,
		RetainedDomainState: *ref.RetainedDomainState,
		PGID:                *ref.PGID,
		Leader:              *ref.Leader,
		Monitor:             *ref.Monitor,
		RetainedID:          *ref.RetainedID,
	}
	if err := groupRef.Validate(); err != nil {
		return model.GroupRef{}, fmt.Errorf("expectedGroupRef: %v", err)
	}
	return groupRef, nil
}
