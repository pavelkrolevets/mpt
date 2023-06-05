package mpt

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pavelkrolevets/mpt/ethdb/memorydb"
)

// Used for testing
func newEmpty() *MerklePatriciaTrie {
	trie, _ := New(common.Hash{}, NewDatabase(memorydb.New()))
	return trie
}

func TestEmptyTrie(t *testing.T) {
	var trie MerklePatriciaTrie
	res := trie.Hash()
	exp := emptyRoot
	if res != exp {
		t.Errorf("expected %x got %x", exp, res)
	}
}

func TestPut(t *testing.T) {
	trie := newEmpty()

	putString(trie, "doe", "reindeer")
	putString(trie, "dog", "puppy")
	putString(trie, "dogglesworth", "cat")

	exp := common.HexToHash("919b9ccfeefcaf9660884cf991fe6daddd66ad32e49ddee5a2a65ea4cb3fbceb")
	root := trie.Hash()
	if root != exp {
		t.Errorf("case 1: exp %x got %x", exp, root)
	}

	trie = newEmpty()
	putString(trie, "A", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	exp = common.HexToHash("c5dd2643cdc69c74763d3f938da62aa8ac4be23420f0e0c8c0f0593368266153")
	root, err := trie.Commit(nil)
	if err != nil {
		t.Fatalf("commit error: %v", err)
	}
	if root != exp {
		t.Errorf("case 2: exp %x got %x", exp, root)
	}
}

func TestDel(t *testing.T) {
	trie := newEmpty()
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"ether", ""},
		{"dog", "puppy"},
		{"shaman", ""},
	}
	for _, val := range vals {
		if val.v != "" {
			putString(trie, val.k, val.v)
		} else {
			deleteString(trie, val.k)
		}
	}

	hash := trie.Hash()
	exp := common.HexToHash("3fd4e9bfe98bd6f58430e469c7a821d9bb5f89fd8aa65267735217325fa2026e")
	if hash != exp {
		t.Errorf("expected %x got %x", exp, hash)
	}
}

func TestGet(t *testing.T) {
	trie := newEmpty()
	putString(trie, "doe", "reindeer")
	putString(trie, "dog", "puppy")
	putString(trie, "dogglesworth", "cat")

	for i := 0; i < 2; i++ {
		res := getString(trie, "dog")
		if !bytes.Equal(res, []byte("puppy")) {
			t.Errorf("expected puppy got %x", res)
		}

		unknown := getString(trie, "unknown")
		if unknown != nil {
			t.Errorf("expected nil got %x", unknown)
		}

		if i == 1 {
			return
		}
		trie.Commit(nil)
	}
}

func TestProof(t *testing.T) {
	trie := newEmpty()

	putString(trie, "doe", "reindeer")
	putString(trie, "dog", "puppy")
	putString(trie, "dogglesworth", "cat")
	putString(trie, "dom", "111")
	putString(trie, "dad", "222")

	root := trie.Hash()
	t.Logf("%x", root)

	key := keybytesToHex([]byte("doe"))
	var nodes []Node
	tn := trie.root
	for len(key) > 0 && tn != nil {
		switch n := tn.(type) {
		case *ShortNode:
			if len(key) < len(n.Key) || !bytes.Equal(n.Key, key[:len(n.Key)]) {
				// The trie doesn't contain the key.
				tn = nil
			} else {
				tn = n.Val
				key = key[len(n.Key):]
			}
			nodes = append(nodes, n)
			t.Logf("Short Node key %v, val %v", n.Key, n.Val)
		case *BranchNode:
			tn = n.Children[key[0]]
			key = key[1:]
			nodes = append(nodes, n)
			t.Logf("Branch Node children %v", n.Children)
		case HashNode:
			var err error
			tn, err = trie.resolveHash(n, nil)
			if err != nil {
				t.Errorf(fmt.Sprintf("Unhandled trie error: %v", err))
			}
		default:
			panic(fmt.Sprintf("%T: invalid node: %v", tn, tn))
		}
	}

	proof, err := trie.Proof([]byte("doe"))
	if err !=nil {
		t.Errorf("got %x", err)
	}
	t.Logf("%v", proof)

}

func putString(trie *MerklePatriciaTrie, k, v string) {
	trie.Put([]byte(k), []byte(v))
}

func deleteString(trie *MerklePatriciaTrie, k string) {
	trie.Del([]byte(k))
}

func getString(trie *MerklePatriciaTrie, k string) []byte {
	return trie.Get([]byte(k))
}