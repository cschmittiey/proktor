package ogg

// crcTable is the lookup table for the Ogg page checksum: CRC-32 with
// polynomial 0x04c11db7, initial value 0, no bit reflection, no final xor.
var crcTable = func() (t [256]uint32) {
	for i := range t {
		r := uint32(i) << 24
		for range 8 {
			if r&0x80000000 != 0 {
				r = r<<1 ^ 0x04c11db7
			} else {
				r <<= 1
			}
		}
		t[i] = r
	}
	return t
}()

func crcUpdate(crc uint32, b []byte) uint32 {
	for _, c := range b {
		crc = crc<<8 ^ crcTable[byte(crc>>24)^c]
	}
	return crc
}

// PageCRC returns the Ogg checksum of a complete page, computed as if the
// four CRC bytes (offsets 22..25) were zero. It panics if page is shorter
// than the 27-byte fixed header.
func PageCRC(page []byte) uint32 {
	crc := crcUpdate(0, page[:22])
	crc = crcUpdate(crc, []byte{0, 0, 0, 0})
	return crcUpdate(crc, page[26:])
}
