package mpt

import (
	"bytes"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pavelkrolevets/mpt/gost3411"
	"github.com/pavelkrolevets/mpt/rlp"
)


func Hash(n Node, force bool) (hashed Node, cached Node) {
	// Trie not processed yet, walk the children
	switch n := n.(type) {
	case *ShortNode:
		collapsed, cached := hashShortNodeChildren(n)
		hashed := shortnodeToHash(collapsed, force)
		// We need to retain the possibly _not_ hashed node, in case it was too
		// small to be hashed
		if hn, ok := hashed.(HashNode); ok {
			cached.flags.hash = hn
		} else {
			cached.flags.hash = nil
		}
		return hashed, cached
	case *BranchNode:
		collapsed, cached := hashBranchNodeChildren(n)
		hashed = branchNodeToHash(collapsed, force)
		if hn, ok := hashed.(HashNode); ok {
			cached.flags.hash = hn
		} else {
			cached.flags.hash = nil
		}
		return hashed, cached
	default:
		// Value and hash nodes don't have children so they're left as were
		return n, n
	}
}

func branchNodeToHash(n *BranchNode, force bool) Node {
	buf := new(bytes.Buffer)
	defer buf.Reset()
	if err := n.EncodeRLP(buf); err != nil {
		panic("encode error: " + err.Error())
	}

	if len(buf.Bytes()) < 32 && !force {
		return n // Nodes smaller than 32 bytes are stored inside their parent
	}

	return hashData(buf.Bytes())
}

func shortnodeToHash(n *ShortNode, force bool) Node {
	buf := new(bytes.Buffer)
	defer buf.Reset()
	if err := rlp.Encode(buf, n); err != nil {
		panic("encode error: " + err.Error())
	}

	if len(buf.Bytes()) < 32 && !force {
		return n // Nodes smaller than 32 bytes are stored inside their parent
	}
	return hashData(buf.Bytes())
}

func hashBranchNodeChildren(n *BranchNode) (collapsed *BranchNode, cached *BranchNode) {
	// Hash the full node's children, caching the newly hashed subtrees
	cached = n.copy()
	collapsed = n.copy()
	for i := 0; i < 16; i++ {
		if child := n.Children[i]; child != nil {
			collapsed.Children[i], cached.Children[i] = Hash(child, false)
		} else {
			collapsed.Children[i] = NilValueNode
		}
	}
	return collapsed, cached
}

func hashShortNodeChildren(n *ShortNode) (collapsed, cached *ShortNode) {
	// Hash the short node's child, caching the newly hashed subtree
	collapsed, cached = n.copy(), n.copy()
	// Previously, we did copy this one. We don't seem to need to actually
	// do that, since we don't overwrite/reuse keys
	//cached.Key = common.CopyBytes(n.Key)
	collapsed.Key = hexToCompact(n.Key)
	// Unless the child is a valuenode or hashnode, hash it
	switch n.Val.(type) {
	case *BranchNode, *ShortNode:
		collapsed.Val, cached.Val = Hash(n.Val, false)
	}
	return collapsed, cached
}

func hashData(data []byte) HashNode {
	hash := gost3411.New256()
	hash.Reset()
	hash.Write(data)
	return hash.Sum(nil)
}

type MissingNodeError struct {
	NodeHash common.Hash // hash of the missing node
	Path     []byte      // hex-encoded path to the missing node
}

func (err *MissingNodeError) Error() string {
	return fmt.Sprintf("missing trie node %x (path %x)", err.NodeHash, err.Path)
}

func proofHash(original Node) (collapsed, hashed Node) {
	switch n := original.(type) {
	case *ShortNode:
		sn, _ := hashShortNodeChildren(n)
		return sn, shortnodeToHash(sn, false)
	case *BranchNode:
		fn, _ := hashBranchNodeChildren(n)
		return fn, branchNodeToHash(fn, false)
	default:
		// Value and hash nodes don't have children so they're left as were
		return n, n
	}
}