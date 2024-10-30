package constants

const BNODE_HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

// static_assert BTREE_PAGE_SIZE bigger than page with one max big key and value
const _ uint = BTREE_PAGE_SIZE -
	(BNODE_HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE)
