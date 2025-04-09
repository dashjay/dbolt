## Bnode


There are two types of bnode, one is `leaf` another is `node`.

The format of the bnode is as following:


```
| type | nkeys |  pointers  |   offsets  | key-values | unused |
|  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |
| headers      | data . . . . . . . . .               |

```

- **type(2Bytes)** can be leaf or (normal) node
- **nkeys** means that this node has n entries(key-value entry or pointer entry)
- **pointers** (for normal node only) store `nkeys` pointers to subtree
- **offsets** (for leaf node only) store the start of key-value pair
- **key-values** (for leaf node only) store key-value pair in following format:

```
| klen | vlen | key | val |
|  2B  |  2B  | ... | ... |
```