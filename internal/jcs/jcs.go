// Package jcs renders JSON using RFC 8785.
package jcs

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Render returns raw rendered according to RFC 8785's JSON Canonicalization
// Scheme. It accepts exactly one UTF-8 JSON value and rejects duplicate object
// member names at every depth.
func Render(raw []byte) ([]byte, error) {
	value, err := parseJCSJSON(raw)
	if err != nil {
		return nil, err
	}
	return canonicalJCSJSON(value)
}

// jcsValue is the deliberately small JSON tree used to render JSON input
// according to RFC 8785. It retains JSON numbers as source text until
// rendering, where they are converted through IEEE-754 binary64 just as
// ECMAScript's Number::toString does.
type jcsValue struct {
	kind   jcsKind
	bool   bool
	string string
	number string
	array  []jcsValue
	object []jcsMember
}

type jcsKind uint8

const (
	jcsNull jcsKind = iota + 1
	jcsBoolean
	jcsString
	jcsNumber
	jcsArray
	jcsObject
)

type jcsMember struct {
	name  string
	value jcsValue
}

// parseJCSJSON accepts exactly one UTF-8 JSON value and rejects duplicate
// object member names at every depth.
func parseJCSJSON(raw []byte) (jcsValue, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return jcsValue{}, fmt.Errorf("json is required")
	}
	if !utf8.Valid(raw) {
		return jcsValue{}, fmt.Errorf("json must be valid UTF-8")
	}

	parser := jcsParser{raw: raw}
	parser.skipSpace()
	value, err := parser.parseValue()
	if err != nil {
		return jcsValue{}, err
	}
	parser.skipSpace()
	if parser.pos != len(parser.raw) {
		return jcsValue{}, parser.errorf("contains multiple top-level values")
	}
	return value, nil
}

func canonicalJCSJSON(value jcsValue) ([]byte, error) {
	var out bytes.Buffer
	if err := writeJCSValue(&out, value); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func writeJCSValue(out *bytes.Buffer, value jcsValue) error {
	switch value.kind {
	case jcsNull:
		out.WriteString("null")
	case jcsBoolean:
		if value.bool {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case jcsString:
		writeJCSString(out, value.string)
	case jcsNumber:
		canonical, err := canonicalJCSNumber(value.number)
		if err != nil {
			return err
		}
		out.WriteString(canonical)
	case jcsArray:
		out.WriteByte('[')
		for i, item := range value.array {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := writeJCSValue(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case jcsObject:
		members := append([]jcsMember(nil), value.object...)
		sort.Slice(members, func(i, j int) bool {
			return lessUTF16(members[i].name, members[j].name)
		})
		out.WriteByte('{')
		for i, member := range members {
			if i > 0 {
				out.WriteByte(',')
			}
			writeJCSString(out, member.name)
			out.WriteByte(':')
			if err := writeJCSValue(out, member.value); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("unsupported JSON value")
	}
	return nil
}

// canonicalJCSNumber follows RFC 8785's reference to ECMAScript
// Number::toString. strconv supplies the shortest binary64 representation;
// the decimal/exponent threshold and exponent spelling below are the
// ECMAScript rules (fixed notation for 1e-6 through 1e20).
func canonicalJCSNumber(raw string) (string, error) {
	value, err := strconv.ParseFloat(raw, 64)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "", fmt.Errorf("number %q is outside the IEEE-754 binary64 range", raw)
	}
	// An underflowed but syntactically valid JSON number is ECMAScript zero.
	if value == 0 {
		return "0", nil
	}
	if err != nil {
		return "", fmt.Errorf("parse number %q: %w", raw, err)
	}

	scientific := strconv.FormatFloat(value, 'e', -1, 64)
	marker := strings.IndexByte(scientific, 'e')
	if marker < 0 {
		return "", fmt.Errorf("render number %q", raw)
	}
	mantissa, exponentText := scientific[:marker], scientific[marker+1:]
	exponent, err := strconv.Atoi(exponentText)
	if err != nil {
		return "", fmt.Errorf("render number exponent %q: %w", raw, err)
	}

	sign := ""
	if mantissa[0] == '-' {
		sign = "-"
		mantissa = mantissa[1:]
	}
	digits := strings.ReplaceAll(mantissa, ".", "")
	if len(digits) == 0 {
		return "", fmt.Errorf("render number %q", raw)
	}

	switch {
	case exponent >= 0 && exponent < 21:
		cut := exponent + 1
		if len(digits) <= cut {
			return sign + digits + strings.Repeat("0", cut-len(digits)), nil
		}
		return sign + digits[:cut] + "." + digits[cut:], nil
	case exponent >= -6 && exponent < 0:
		return sign + "0." + strings.Repeat("0", -exponent-1) + digits, nil
	default:
		result := sign + digits[:1]
		if len(digits) > 1 {
			result += "." + digits[1:]
		}
		result += "e"
		if exponent >= 0 {
			result += "+"
		}
		return result + strconv.Itoa(exponent), nil
	}
}

func writeJCSString(out *bytes.Buffer, value string) {
	const hex = "0123456789abcdef"

	out.WriteByte('"')
	for _, runeValue := range value {
		switch runeValue {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\t':
			out.WriteString(`\t`)
		case '\n':
			out.WriteString(`\n`)
		case '\f':
			out.WriteString(`\f`)
		case '\r':
			out.WriteString(`\r`)
		default:
			if runeValue <= 0x1f {
				out.WriteString(`\u00`)
				out.WriteByte(hex[(runeValue>>4)&0x0f])
				out.WriteByte(hex[runeValue&0x0f])
				continue
			}
			out.WriteRune(runeValue)
		}
	}
	out.WriteByte('"')
}

// lessUTF16 compares strings by their UTF-16 code units, not Go's UTF-8
// bytes or Unicode code points. RFC 8785 requires this exact member order.
func lessUTF16(left, right string) bool {
	leftUnits := utf16.Encode([]rune(left))
	rightUnits := utf16.Encode([]rune(right))
	limit := len(leftUnits)
	if len(rightUnits) < limit {
		limit = len(rightUnits)
	}
	for i := 0; i < limit; i++ {
		if leftUnits[i] != rightUnits[i] {
			return leftUnits[i] < rightUnits[i]
		}
	}
	return len(leftUnits) < len(rightUnits)
}

type jcsParser struct {
	raw []byte
	pos int
}

func (parser *jcsParser) parseValue() (jcsValue, error) {
	if parser.pos == len(parser.raw) {
		return jcsValue{}, parser.errorf("unexpected end of JSON")
	}

	switch parser.raw[parser.pos] {
	case 'n':
		if !parser.consumeLiteral("null") {
			return jcsValue{}, parser.errorf("invalid JSON literal")
		}
		return jcsValue{kind: jcsNull}, nil
	case 't':
		if !parser.consumeLiteral("true") {
			return jcsValue{}, parser.errorf("invalid JSON literal")
		}
		return jcsValue{kind: jcsBoolean, bool: true}, nil
	case 'f':
		if !parser.consumeLiteral("false") {
			return jcsValue{}, parser.errorf("invalid JSON literal")
		}
		return jcsValue{kind: jcsBoolean}, nil
	case '"':
		value, err := parser.parseString()
		if err != nil {
			return jcsValue{}, err
		}
		return jcsValue{kind: jcsString, string: value}, nil
	case '[':
		return parser.parseArray()
	case '{':
		return parser.parseObject()
	default:
		if parser.raw[parser.pos] == '-' || (parser.raw[parser.pos] >= '0' && parser.raw[parser.pos] <= '9') {
			return parser.parseNumber()
		}
		return jcsValue{}, parser.errorf("invalid JSON value")
	}
}

func (parser *jcsParser) parseObject() (jcsValue, error) {
	parser.pos++ // {
	parser.skipSpace()
	if parser.consumeByte('}') {
		return jcsValue{kind: jcsObject}, nil
	}

	seen := make(map[string]struct{})
	members := make([]jcsMember, 0)
	for {
		parser.skipSpace()
		if parser.pos == len(parser.raw) || parser.raw[parser.pos] != '"' {
			return jcsValue{}, parser.errorf("object member name must be a string")
		}
		name, err := parser.parseString()
		if err != nil {
			return jcsValue{}, err
		}
		if _, exists := seen[name]; exists {
			return jcsValue{}, parser.errorf("duplicate object member %q", name)
		}
		seen[name] = struct{}{}

		parser.skipSpace()
		if !parser.consumeByte(':') {
			return jcsValue{}, parser.errorf("object member is missing ':'")
		}
		parser.skipSpace()
		value, err := parser.parseValue()
		if err != nil {
			return jcsValue{}, err
		}
		members = append(members, jcsMember{name: name, value: value})

		parser.skipSpace()
		if parser.consumeByte('}') {
			return jcsValue{kind: jcsObject, object: members}, nil
		}
		if !parser.consumeByte(',') {
			return jcsValue{}, parser.errorf("object is not closed")
		}
		parser.skipSpace()
	}
}

func (parser *jcsParser) parseArray() (jcsValue, error) {
	parser.pos++ // [
	parser.skipSpace()
	if parser.consumeByte(']') {
		return jcsValue{kind: jcsArray}, nil
	}

	items := make([]jcsValue, 0)
	for {
		parser.skipSpace()
		item, err := parser.parseValue()
		if err != nil {
			return jcsValue{}, err
		}
		items = append(items, item)

		parser.skipSpace()
		if parser.consumeByte(']') {
			return jcsValue{kind: jcsArray, array: items}, nil
		}
		if !parser.consumeByte(',') {
			return jcsValue{}, parser.errorf("array is not closed")
		}
		parser.skipSpace()
	}
}

func (parser *jcsParser) parseString() (string, error) {
	parser.pos++ // opening quote
	var out bytes.Buffer
	for parser.pos < len(parser.raw) {
		current := parser.raw[parser.pos]
		switch {
		case current == '"':
			parser.pos++
			return out.String(), nil
		case current < 0x20:
			return "", parser.errorf("unescaped control character in string")
		case current == '\\':
			parser.pos++
			if parser.pos == len(parser.raw) {
				return "", parser.errorf("incomplete string escape")
			}
			escaped := parser.raw[parser.pos]
			parser.pos++
			switch escaped {
			case '"', '\\', '/':
				out.WriteByte(map[byte]byte{'"': '"', '\\': '\\', '/': '/'}[escaped])
			case 'b':
				out.WriteByte('\b')
			case 'f':
				out.WriteByte('\f')
			case 'n':
				out.WriteByte('\n')
			case 'r':
				out.WriteByte('\r')
			case 't':
				out.WriteByte('\t')
			case 'u':
				codeUnit, err := parser.parseHexCodeUnit()
				if err != nil {
					return "", err
				}
				if codeUnit >= 0xd800 && codeUnit <= 0xdbff {
					if parser.pos+2 > len(parser.raw) || parser.raw[parser.pos] != '\\' || parser.raw[parser.pos+1] != 'u' {
						return "", parser.errorf("unpaired high surrogate")
					}
					parser.pos += 2
					lowSurrogate, err := parser.parseHexCodeUnit()
					if err != nil {
						return "", err
					}
					if lowSurrogate < 0xdc00 || lowSurrogate > 0xdfff {
						return "", parser.errorf("unpaired high surrogate")
					}
					out.WriteRune(rune(0x10000) + (rune(codeUnit)-0xd800)*0x400 + (rune(lowSurrogate) - 0xdc00))
				} else if codeUnit >= 0xdc00 && codeUnit <= 0xdfff {
					return "", parser.errorf("unpaired low surrogate")
				} else {
					out.WriteRune(rune(codeUnit))
				}
			default:
				return "", parser.errorf("invalid string escape")
			}
		default:
			if current < utf8.RuneSelf {
				out.WriteByte(current)
				parser.pos++
				continue
			}
			runeValue, width := utf8.DecodeRune(parser.raw[parser.pos:])
			if runeValue == utf8.RuneError && width == 1 {
				return "", parser.errorf("invalid UTF-8 in string")
			}
			out.WriteRune(runeValue)
			parser.pos += width
		}
	}
	return "", parser.errorf("unterminated string")
}

func (parser *jcsParser) parseNumber() (jcsValue, error) {
	start := parser.pos
	if parser.consumeByte('-') && parser.pos == len(parser.raw) {
		return jcsValue{}, parser.errorf("incomplete number")
	}
	if parser.pos == len(parser.raw) {
		return jcsValue{}, parser.errorf("incomplete number")
	}

	switch parser.raw[parser.pos] {
	case '0':
		parser.pos++
		if parser.pos < len(parser.raw) && parser.raw[parser.pos] >= '0' && parser.raw[parser.pos] <= '9' {
			return jcsValue{}, parser.errorf("leading zero in number")
		}
	case '1', '2', '3', '4', '5', '6', '7', '8', '9':
		for parser.pos < len(parser.raw) && parser.raw[parser.pos] >= '0' && parser.raw[parser.pos] <= '9' {
			parser.pos++
		}
	default:
		return jcsValue{}, parser.errorf("invalid number")
	}

	if parser.consumeByte('.') {
		fractionStart := parser.pos
		for parser.pos < len(parser.raw) && parser.raw[parser.pos] >= '0' && parser.raw[parser.pos] <= '9' {
			parser.pos++
		}
		if fractionStart == parser.pos {
			return jcsValue{}, parser.errorf("number fraction is empty")
		}
	}
	if parser.pos < len(parser.raw) && (parser.raw[parser.pos] == 'e' || parser.raw[parser.pos] == 'E') {
		parser.pos++
		if parser.pos < len(parser.raw) && (parser.raw[parser.pos] == '+' || parser.raw[parser.pos] == '-') {
			parser.pos++
		}
		exponentStart := parser.pos
		for parser.pos < len(parser.raw) && parser.raw[parser.pos] >= '0' && parser.raw[parser.pos] <= '9' {
			parser.pos++
		}
		if exponentStart == parser.pos {
			return jcsValue{}, parser.errorf("number exponent is empty")
		}
	}

	raw := string(parser.raw[start:parser.pos])
	if _, err := canonicalJCSNumber(raw); err != nil {
		return jcsValue{}, err
	}
	return jcsValue{kind: jcsNumber, number: raw}, nil
}

func (parser *jcsParser) parseHexCodeUnit() (uint16, error) {
	if parser.pos+4 > len(parser.raw) {
		return 0, parser.errorf("incomplete Unicode escape")
	}
	var value uint16
	for i := 0; i < 4; i++ {
		digit, ok := hexDigit(parser.raw[parser.pos+i])
		if !ok {
			return 0, parser.errorf("invalid Unicode escape")
		}
		value = value<<4 | uint16(digit)
	}
	parser.pos += 4
	return value, nil
}

func hexDigit(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

func (parser *jcsParser) consumeLiteral(literal string) bool {
	if len(parser.raw)-parser.pos < len(literal) || string(parser.raw[parser.pos:parser.pos+len(literal)]) != literal {
		return false
	}
	parser.pos += len(literal)
	return true
}

func (parser *jcsParser) consumeByte(want byte) bool {
	if parser.pos == len(parser.raw) || parser.raw[parser.pos] != want {
		return false
	}
	parser.pos++
	return true
}

func (parser *jcsParser) skipSpace() {
	for parser.pos < len(parser.raw) {
		switch parser.raw[parser.pos] {
		case ' ', '\t', '\n', '\r':
			parser.pos++
		default:
			return
		}
	}
}

func (parser *jcsParser) errorf(format string, args ...any) error {
	return fmt.Errorf("json at byte %d: %s", parser.pos, fmt.Sprintf(format, args...))
}
