package buffer_test

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/errors"
)

func TestReaderReadRune32PreservesFixedWidthEncoding(t *testing.T) {
	tests := []struct {
		name  string
		order binary.ByteOrder
		data  []byte
		want  rune
	}{
		{name: "big endian CJK", order: binary.BigEndian, data: []byte{0, 0, 0x75, 0x4c, 0xaa}, want: '界'},
		{name: "little endian CJK", order: binary.LittleEndian, data: []byte{0x4c, 0x75, 0, 0, 0xaa}, want: '界'},
		{name: "supplementary rune", order: binary.BigEndian, data: []byte{0, 1, 0xf6, 0x80, 0xaa}, want: '🚀'},
		{name: "signed int32", order: binary.LittleEndian, data: []byte{0xff, 0xff, 0xff, 0xff, 0xaa}, want: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := buffer.NewReader(tt.data)
			got, err := reader.ReadRune32(tt.order)
			if err != nil || got != tt.want {
				t.Fatalf("ReadRune32() = %U, %v; want %U", got, err, tt.want)
			}
			marker, err := reader.ReadByte()
			if err != nil || marker != 0xaa {
				t.Fatal("ReadRune32 did not consume exactly four bytes")
			}
		})
	}
}

func TestReaderReadRune32ShortInputDoesNotAdvance(t *testing.T) {
	for size := range 4 {
		reader := buffer.NewReader([]byte{1, 2, 3}[:size])
		got, err := reader.ReadRune32(binary.BigEndian)
		if got != 0 || !errors.Is(err, errors.ErrUnexpectedEOF) {
			t.Fatalf("size %d: ReadRune32() = %v, %v; want zero and ErrUnexpectedEOF", size, got, err)
		}
		offset, err := reader.Seek(0, io.SeekCurrent)
		if err != nil || offset != 0 {
			t.Fatalf("size %d: short read moved the cursor to %d: %v", size, offset, err)
		}
	}
}
