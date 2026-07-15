package parkproto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	if err := json.Unmarshal(frame, &env); err != nil {
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
		if err := json.Unmarshal(payload, &report); err != nil {
			return nil, fmt.Errorf("%w: identity report: %v", ErrMalformed, err)
		}
		if err := report.Validate(); err != nil {
			return nil, fmt.Errorf("%w: identity report: %v", ErrMalformed, err)
		}
		return report, nil
	case MessageRelease:
		var release Release
		if err := json.Unmarshal(payload, &release); err != nil {
			return nil, fmt.Errorf("%w: release: %v", ErrMalformed, err)
		}
		return release, nil
	case MessageReleaseAck:
		var ack ReleaseAck
		if err := json.Unmarshal(payload, &ack); err != nil {
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
