package interop

func AddrUInt16(i uint16) *uint16 {
	return &i
}

func AddrInt32(i int32) *int32 {
	return &i
}

func AddrString(s string) *string {
	return &s
}
