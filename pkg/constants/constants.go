package constants

import "encoding/binary"

const BNodeHeader = 4

const BtreePageSize = 4096
const BtreePageSizeFor2 = BtreePageSize * 2
const BtreeMaxKeySize = 1000
const BtreeMaxValSize = 3000

const Uint16Size = 2
const Uint32Size = 4
const Uint64Size = 8

// static_assert BtreePageSize bigger than page with one max big key and value
const _ uint = BtreePageSize -
	(BNodeHeader + 8 + 2 + 4 + BtreeMaxKeySize + BtreeMaxValSize)

//nolint:gochecknoglobals // avoid to use the wrong binary algorithm
var BinaryAlgorithm = binary.LittleEndian

const MetaKeyNextPrefix = "next_prefix"

const MinTablePrefix = 3
