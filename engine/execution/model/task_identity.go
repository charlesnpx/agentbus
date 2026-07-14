package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"unicode/utf8"
)

const taskSpecIdentityDomainV1 = "agentbus/task-spec/sha256/v1\x00"

// TaskIdentityCodec derives the durable identity recorded in bindings and
// tombstones from the raw taskSpec JSON object.
type TaskIdentityCodec interface {
	Version() uint16
	FromRawTaskSpec(json.RawMessage) (TaskIdentity, error)
}

type TaskIdentityCodecV1 struct{}

func CurrentTaskIdentityCodec() TaskIdentityCodec {
	return TaskIdentityCodecV1{}
}

func TaskIdentityCodecFor(algorithm string, version uint16) (TaskIdentityCodec, error) {
	if algorithm != TaskIdentityAlgorithmSHA256 {
		return nil, invalid("task_identity.algorithm", "is unsupported")
	}
	if version != CurrentTaskIdentityVersion {
		return nil, invalid("task_identity.version", "is unsupported")
	}
	return TaskIdentityCodecV1{}, nil
}

func (TaskIdentityCodecV1) Version() uint16 {
	return CurrentTaskIdentityVersion
}

func (codec TaskIdentityCodecV1) FromRawTaskSpec(raw json.RawMessage) (TaskIdentity, error) {
	canonical, err := CanonicalTaskSpecJSON(raw)
	if err != nil {
		return TaskIdentity{}, err
	}
	sum := sha256.Sum256(append([]byte(taskSpecIdentityDomainV1), canonical...))
	return NewTaskIdentity(TaskIdentityAlgorithmSHA256, codec.Version(), hex.EncodeToString(sum[:]))
}

func TaskIdentityFromRawTaskSpec(raw json.RawMessage) (TaskIdentity, error) {
	return CurrentTaskIdentityCodec().FromRawTaskSpec(raw)
}

func TaskIdentityMatchesRawTaskSpec(recorded TaskIdentity, raw json.RawMessage) error {
	if err := recorded.Validate(); err != nil {
		return err
	}
	codec, err := TaskIdentityCodecFor(recorded.Algorithm, recorded.Version)
	if err != nil {
		return err
	}
	current, err := codec.FromRawTaskSpec(raw)
	if err != nil {
		return err
	}
	if !recorded.Equal(current) {
		return invalid("task_identity.value", "does not match raw taskSpec")
	}
	return nil
}

// ExtractRawTaskSpec returns params.taskSpec from a raw JSON-RPC-style
// envelope. It is a pure helper and does not perform admission, filesystem, or
// backend validation.
func ExtractRawTaskSpec(envelope []byte) (json.RawMessage, error) {
	root, err := parseCanonicalJSONDocument(envelope)
	if err != nil {
		return nil, err
	}
	if root.kind != canonicalJSONObject {
		return nil, invalid("json", "must be an object")
	}

	var rawRoot map[string]json.RawMessage
	if err := decodeRawJSONDocument(envelope, &rawRoot); err != nil {
		return nil, err
	}
	paramsRaw, ok := rawRoot["params"]
	if !ok {
		return nil, invalid("params", "is required")
	}

	var rawParams map[string]json.RawMessage
	if err := decodeRawJSONDocument(paramsRaw, &rawParams); err != nil {
		return nil, invalid("params", "must be an object")
	}
	taskSpecRaw, ok := rawParams["taskSpec"]
	if !ok {
		return nil, invalid("params.taskSpec", "is required")
	}

	taskSpec, err := parseCanonicalJSONDocument(taskSpecRaw)
	if err != nil {
		return nil, err
	}
	if taskSpec.kind != canonicalJSONObject {
		return nil, invalid("params.taskSpec", "must be a JSON object")
	}
	return append(json.RawMessage(nil), taskSpecRaw...), nil
}

// CanonicalTaskSpecJSON renders the raw taskSpec object for identity hashing.
// V1 performs no semantic defaulting: omitted fields, explicit nulls, empty
// values, and numeric zero are distinct raw inputs and therefore distinct
// identities.
func CanonicalTaskSpecJSON(raw json.RawMessage) ([]byte, error) {
	node, err := parseCanonicalJSONDocument(raw)
	if err != nil {
		return nil, err
	}
	if node.kind != canonicalJSONObject {
		return nil, invalid("task_spec", "must be a JSON object")
	}
	var out bytes.Buffer
	writeCanonicalJSON(&out, node)
	return out.Bytes(), nil
}

type canonicalJSONKind uint8

const (
	canonicalJSONNull canonicalJSONKind = iota + 1
	canonicalJSONBool
	canonicalJSONString
	canonicalJSONNumber
	canonicalJSONArray
	canonicalJSONObject
)

type canonicalJSONNode struct {
	kind    canonicalJSONKind
	boolean bool
	text    string
	array   []canonicalJSONNode
	object  []canonicalJSONMember
}

type canonicalJSONMember struct {
	key   string
	value canonicalJSONNode
}

func parseCanonicalJSONDocument(raw []byte) (canonicalJSONNode, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return canonicalJSONNode{}, invalid("json", "is required")
	}
	if !utf8.Valid(raw) {
		return canonicalJSONNode{}, invalid("json", "must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	node, err := parseCanonicalJSONValue(decoder, "json")
	if err != nil {
		if err == io.EOF {
			return canonicalJSONNode{}, invalid("json", "is required")
		}
		return canonicalJSONNode{}, err
	}
	if err := rejectTrailingJSON(decoder); err != nil {
		return canonicalJSONNode{}, err
	}
	return node, nil
}

func parseCanonicalJSONValue(decoder *json.Decoder, path string) (canonicalJSONNode, error) {
	token, err := decoder.Token()
	if err != nil {
		return canonicalJSONNode{}, err
	}
	switch value := token.(type) {
	case nil:
		return canonicalJSONNode{kind: canonicalJSONNull}, nil
	case bool:
		return canonicalJSONNode{kind: canonicalJSONBool, boolean: value}, nil
	case string:
		return canonicalJSONNode{kind: canonicalJSONString, text: value}, nil
	case json.Number:
		if value.String() == "" {
			return canonicalJSONNode{}, invalid(path, "number is empty")
		}
		return canonicalJSONNode{kind: canonicalJSONNumber, text: value.String()}, nil
	case json.Delim:
		switch value {
		case '{':
			return parseCanonicalJSONObject(decoder, path)
		case '[':
			return parseCanonicalJSONArray(decoder, path)
		default:
			return canonicalJSONNode{}, invalid(path, fmt.Sprintf("unexpected delimiter %q", value))
		}
	default:
		return canonicalJSONNode{}, invalid(path, fmt.Sprintf("unexpected JSON token %T", token))
	}
}

func parseCanonicalJSONObject(decoder *json.Decoder, path string) (canonicalJSONNode, error) {
	seen := map[string]struct{}{}
	members := []canonicalJSONMember{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return canonicalJSONNode{}, err
		}
		key, ok := token.(string)
		if !ok {
			return canonicalJSONNode{}, invalid(path, "object key must be a string")
		}
		if _, exists := seen[key]; exists {
			return canonicalJSONNode{}, invalid(path, fmt.Sprintf("duplicate key %q", key))
		}
		seen[key] = struct{}{}
		value, err := parseCanonicalJSONValue(decoder, path+"."+key)
		if err != nil {
			return canonicalJSONNode{}, err
		}
		members = append(members, canonicalJSONMember{key: key, value: value})
	}
	end, err := decoder.Token()
	if err != nil {
		return canonicalJSONNode{}, err
	}
	if delim, ok := end.(json.Delim); !ok || delim != '}' {
		return canonicalJSONNode{}, invalid(path, "object is not closed")
	}
	return canonicalJSONNode{kind: canonicalJSONObject, object: members}, nil
}

func parseCanonicalJSONArray(decoder *json.Decoder, path string) (canonicalJSONNode, error) {
	values := []canonicalJSONNode{}
	for decoder.More() {
		value, err := parseCanonicalJSONValue(decoder, path+"[]")
		if err != nil {
			return canonicalJSONNode{}, err
		}
		values = append(values, value)
	}
	end, err := decoder.Token()
	if err != nil {
		return canonicalJSONNode{}, err
	}
	if delim, ok := end.(json.Delim); !ok || delim != ']' {
		return canonicalJSONNode{}, invalid(path, "array is not closed")
	}
	return canonicalJSONNode{kind: canonicalJSONArray, array: values}, nil
}

func rejectTrailingJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return invalid("json", fmt.Sprintf("contains multiple top-level values starting with %v", token))
}

func writeCanonicalJSON(out *bytes.Buffer, node canonicalJSONNode) {
	switch node.kind {
	case canonicalJSONNull:
		out.WriteString("null")
	case canonicalJSONBool:
		if node.boolean {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case canonicalJSONString:
		encoded, _ := json.Marshal(node.text)
		out.Write(encoded)
	case canonicalJSONNumber:
		out.WriteString(node.text)
	case canonicalJSONArray:
		out.WriteByte('[')
		for i, value := range node.array {
			if i > 0 {
				out.WriteByte(',')
			}
			writeCanonicalJSON(out, value)
		}
		out.WriteByte(']')
	case canonicalJSONObject:
		members := append([]canonicalJSONMember(nil), node.object...)
		sort.Slice(members, func(i, j int) bool {
			return members[i].key < members[j].key
		})
		out.WriteByte('{')
		for i, member := range members {
			if i > 0 {
				out.WriteByte(',')
			}
			encoded, _ := json.Marshal(member.key)
			out.Write(encoded)
			out.WriteByte(':')
			writeCanonicalJSON(out, member.value)
		}
		out.WriteByte('}')
	}
}

func decodeRawJSONDocument(raw []byte, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return invalid("json", "contains multiple top-level values")
		}
		return err
	}
	return nil
}
