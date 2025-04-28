#include <stdlib.h>
#include <cstdint>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <xmmintrin.h>
#include <immintrin.h>
#include <emmintrin.h>
#include <smmintrin.h>
#include <bit>
#include <functional>
#include <mutex>
#include <vector>

#include <atomic>

// Define Clock and Key types
typedef std::chrono::high_resolution_clock Clock;
typedef uint64_t Key;

// Compare function for keys
int compare_(const Key& a, const Key& b) {
    if (a < b) {
        return -1;
    } else if (a > b) {
        return +1;
    } else {
        return 0;
    }
}

// B+ Tree class template definition
template<typename Key>
class Bplustree {
   private:
    // Forward declaration of node structures
    struct Node;
    struct InternalNode;
    struct LeafNode;

   public:
    // Constructor: Initializes a B+ Tree with the specified degree (maximum number of children per internal node)
    Bplustree(int degree = 4);

    // Insert function:
    // Inserts a key into the B+ Tree.
    // TODO: Implement insertion, handling leaf node insertion and splitting if necessary.
    void Insert(const Key& key);

    // Contains function:
    // Returns true if the key exists in the tree; otherwise, returns false.
    // TODO: Implement key lookup starting from the root and traversing to the appropriate leaf.
    bool Contains(const Key& key) const;

    // Scan function:
    // Performs a range query starting from the specified key and returns up to 'scan_num' keys.
    // TODO: Traverse leaf nodes using the next pointer and collect keys.
    std::vector<Key> Scan(const Key& key, const int scan_num);

    // Delete function:
    // Removes the specified key from the tree.
    // TODO: Implement deletion, handling key removal, merging, or rebalancing nodes if required.
    bool Delete(const Key& key);

    // Print function:
    // Traverses and prints the internal structure of the B+ Tree.
    // This function is helpful for debugging and verifying that the tree is constructed correctly.
    void Print() const;

   private:
    // Base Node structure. All nodes (internal and leaf) derive from this.
    struct Node {
        bool is_leaf; // Indicates whether the node is a leaf
        // Helper functions to cast a Node pointer to InternalNode or LeafNode pointers.
        InternalNode* as_internal() { return static_cast<InternalNode*>(this); }
        LeafNode* as_leaf() { return static_cast<LeafNode*>(this); }
        virtual ~Node() = default;
    };

    // Internal node structure for the B+ Tree.
    // Stores keys and child pointers.
    struct InternalNode : public Node {
        std::vector<Key> keys;         // Keys used to direct search to the correct child
        std::vector<Node*> children;   // Pointers to child nodes
        InternalNode() { this->is_leaf = false; }
    };

    // Leaf node structure for the B+ Tree.
    // Stores actual keys and a pointer to the next leaf for efficient range queries.
    struct LeafNode : public Node {
        std::vector<Key> keys; // Keys stored in the leaf node
        LeafNode* next;        // Pointer to the next leaf node for range scanning
        LeafNode() : next(nullptr) { this->is_leaf = true; }
    };

    // Helper function to insert a key into an internal node.
    // 'new_child' and 'new_key' are output parameters if the node splits.
    // TODO: Implement insertion into an internal node and handle splitting of nodes.
    void InsertInternal(Node* current, const Key& key, Node*& new_child, Key& new_key);

    // Helper function to delete a key from the tree recursively.
    // TODO: Implement deletion from internal nodes with proper merging or rebalancing.
    bool DeleteInternal(Node* current, const Key& key);

    // Helper function to find the leaf node where the key should reside.
    // TODO: Implement traversal from the root to the appropriate leaf node.
    LeafNode* FindLeaf(const Key& key) const;

    // Helper function to recursively print the tree structure.
    void PrintRecursive(const Node* node, int level) const;

    Node* root;   // Root node of the B+ Tree
    int degree;   // Maximum number of children per internal node
};

// Constructor implementation
// Initializes the tree by creating an empty leaf node as the root.
template<typename Key>
Bplustree<Key>::Bplustree(int degree) : degree(degree) {
    root = new LeafNode();
}

// Insert function: Inserts a key into the B+ Tree.
template<typename Key>
void Bplustree<Key>::Insert(const Key& key) {
    // 새로운 자식 노드와 Key를 저장할 변수들 초기화
    Node* new_child = nullptr;
    Key new_key;

    // 현재 노드가 Leaf node인 경우
    if (root->is_leaf) {
        // root를 LeafNode로 타입 변환
        LeafNode* leaf = root->as_leaf();

        // Leaf node가 가득 찼는지 확인 (degree-1은 최대 Key 개수)
        if (leaf->keys.size() >= static_cast<size_t>(degree - 1)) {
            // Leaf node 분할을 위한 새로운 Leaf node 생성
            LeafNode* new_leaf = new LeafNode();

            // 키를 반으로 나누어 새 Leaf node에 할당
            // split_pos: 분할 위치 계산 (중간 지점)
            size_t split_pos = (degree - 1) / 2;
            // 기존 Leaf node의 후반부 키들을 새 Leaf node로 이동
            new_leaf->keys.assign(leaf->keys.begin() + split_pos, leaf->keys.end());
            // 기존 Leaf node의 키 개수를 split_pos로 조정
            leaf->keys.resize(split_pos);

            // 분할된 Leaf node들 연결 (링크드 리스트 형태)
            new_leaf->next = leaf->next;
            leaf->next = new_leaf;
            
            // 새로운 Internal node 생성 (root node 업그레이드)
            InternalNode* new_root = new InternalNode();
            // 새 Leaf node의 첫 번째 키를 구분자로 사용
            new_root->keys.push_back(new_leaf->keys[0]);
            // 자식 노드들 연결
            new_root->children.push_back(leaf);
            new_root->children.push_back(new_leaf);
            // root node 업데이트
            root = new_root;
        }

        // Key를 정렬된 위치에 삽입
        // lower_bound: key가 삽입될 수 있는 첫 번째 위치 찾기
        auto pos = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
        leaf->keys.insert(pos, key);
    }
    // 현재 노드가 Internal node인 경우
    else {
        // root를 InternalNode로 타입 변환
        InternalNode* internal = root->as_internal();
        // 적절한 자식 노드를 찾기 위한 인덱스
        size_t i = 0;
        // key보다 큰 첫 번째 키를 찾을 때까지 반복
        while (i < internal->keys.size() && compare_(key, internal->keys[i]) > 0) {
            i++;
        }

        // 재귀적으로 적절한 자식 노드에 Key 삽입
        InsertInternal(internal->children[i], key, new_child, new_key);
    }        
}


// Contains function: Checks if a key exists in the B+ Tree.
template<typename Key>
bool Bplustree<Key>::Contains(const Key& key) const {
    // TODO: Implement lookup logic here.
    // To be implemented by students
    LeafNode* leaf = FindLeaf(key); // Key가 있을 수 있는 Leaf Node 찾기
    if (leaf == nullptr) {
        return false;
    }
    // 찾은 Leaf Node 에서 Key 존재 여부 확인
    return std::binary_search(leaf->keys.begin(), leaf->keys.end(), key);
}


// Scan function: Performs a range query starting from a given key.
template<typename Key>
std::vector<Key> Bplustree<Key>::Scan(const Key& key, const int scan_num) {
    // 결과를 저장할 vector 초기화
    std::vector<Key> result;
    
    // 시작 키가 있는 Leaf Node 찾기
    LeafNode* leaf = FindLeaf(key);
    
    // Leaf Node가 없으면 빈 결과 반환
    if (leaf == nullptr) {
        return result;
    }

    // 시작 키 찾기
    // lower_bound: key보다 크거나 같은 첫 번째 위치 찾기
    auto start = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);

    // 현재 Leaf node에서 Key 수집
    // scan_num 개수만큼 또는 현재 Leaf node의 끝까지
    while (start != leaf->keys.end() && result.size() < static_cast<size_t>(scan_num)) {
        result.push_back(*start);
        ++start;
    }

    // 다음 Leaf Node로 이동하면서 Key 수집
    // scan_num 개수만큼 또는 더 이상 Leaf node가 없을 때까지
    while (result.size() < static_cast<size_t>(scan_num) && leaf->next != nullptr) {
        // 다음 Leaf node로 이동
        leaf = leaf->next;
        // 현재 Leaf node의 모든 키 순회
        for (const Key& k : leaf->keys) {
            // scan_num 개수에 도달하면 중단
            if (result.size() >= static_cast<size_t>(scan_num)) {
                break;
            }
            result.push_back(k);
        }
    }   
    
    // 수집된 결과 반환
    return result;
}


// Delete function: Removes a key from the B+ Tree.
template<typename Key>
bool Bplustree<Key>::Delete(const Key& key) {
    // Root node부터 삭제 시작
    // DeleteInternal: 재귀적으로 key를 찾아 삭제하고, 필요한 경우 노드 병합 수행
    return DeleteInternal(root, key);
}


// InsertInternal function: Helper function to insert a key into an internal node.
template<typename Key>
void Bplustree<Key>::InsertInternal(Node* current, const Key& key, Node*& new_child, Key& new_key) {
    // 현재 노드가 Leaf node인 경우
    if (current->is_leaf) {
        // LeafNode로 타입 변환
        LeafNode* leaf = current->as_leaf();

        // Leaf node가 가득 찼는지 확인
        if (leaf->keys.size() >= static_cast<size_t>(degree - 1)) {
            // Leaf node 분할을 위한 새로운 Leaf node 생성
            LeafNode* new_leaf = new LeafNode();
            // 분할 위치 계산
            size_t split_pos = (degree - 1) / 2;

            // Key 분배
            // 기존 Leaf node의 후반부 키들을 새 Leaf node로 이동
            new_leaf->keys.assign(leaf->keys.begin() + split_pos, leaf->keys.end());
            // 기존 Leaf node의 키 개수를 split_pos로 조정
            leaf->keys.resize(split_pos);

            // 분할된 Leaf node들 연결
            new_leaf->next = leaf->next;
            leaf->next = new_leaf;
            
            // 분할 정보 반환
            new_child = new_leaf;
            new_key = new_leaf->keys[0];
        }

        // Key를 정렬된 위치에 삽입
        auto pos = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
        leaf->keys.insert(pos, key);
    } else {
        // Internal node에서 적절한 자식 노드 찾기
        InternalNode* internal = current->as_internal();
        size_t i = 0;
        // key보다 큰 첫 번째 키를 찾을 때까지 반복
        while (i < internal->keys.size() && key >= internal->keys[i]) {
            i++;
        }

        // 재귀적으로 자식 노드에 삽입
        InsertInternal(internal->children[i], key, new_child, new_key);

        // 자식 노드 분할 체크
        if (new_child != nullptr) {
            // Internal node가 가득 찼는지 확인
            if (internal->keys.size() >= static_cast<size_t>(degree - 1)) {
                // Internal node 분할을 위한 새로운 Internal node 생성
                InternalNode* new_internal = new InternalNode();
                // 분할 위치 계산
                size_t split_pos = (degree - 1) / 2;

                // Key와 자식 노드 분배
                // 기존 Internal node의 후반부 키들을 새 Internal node로 이동
                new_internal->keys.assign(internal->keys.begin() + split_pos, internal->keys.end());
                // 기존 Internal node의 후반부 자식들을 새 Internal node로 이동
                new_internal->children.assign(internal->children.begin() + split_pos, internal->children.end());
                // 기존 Internal node의 키와 자식 개수 조정
                internal->keys.resize(split_pos);
                internal->children.resize(split_pos + 1);

                // 분할 정보 반환
                new_child = new_internal;
                new_key = internal->keys[split_pos];
            } else {
                // 분할이 필요 없는 경우
                // Key와 자식 노드를 적절한 위치에 삽입
                auto pos = std::lower_bound(internal->keys.begin(), internal->keys.end(), new_key);
                internal->keys.insert(pos, new_key);
                internal->children.insert(internal->children.begin() + (pos - internal->keys.begin() + 1), new_child);
                new_child = nullptr;
            }
        }
    }
}


// DeleteInternal function: Helper function to delete a key from an internal node.
template<typename Key>
bool Bplustree<Key>::DeleteInternal(Node* current, const Key& key) {
    // 현재 노드가 Leaf node인 경우
    if (current->is_leaf) {
        // LeafNode로 타입 변환
        LeafNode* leaf = current->as_leaf();
        // key를 찾아서 삭제
        auto it = std::find(leaf->keys.begin(), leaf->keys.end(), key);
        // key가 없으면 false 반환
        if (it != leaf->keys.end()) {
            return false;
        }
        // key 삭제
        leaf->keys.erase(it);
        return true;
    } else {
        // Internal node에서 적절한 자식 노드 찾기
        InternalNode* internal = current->as_internal();
        size_t i = 0;
        // key보다 큰 첫 번째 키를 찾을 때까지 반복
        while (i < internal->keys.size() && key >= internal->keys[i]) {
            i++;
        }

        // 재귀적으로 자식 노드에서 삭제
        bool deleted = DeleteInternal(internal->children[i], key);
        // 삭제 실패 시 false 반환
        if (!deleted) return false;

        // 자식 노드가 Leaf node인 경우
        if (internal->children[i]->is_leaf) {
            LeafNode* child = internal->children[i]->as_leaf();
            // 자식 노드의 키 수가 최솟값보다 작은지 확인
            if (child->keys.size() < static_cast<size_t>((degree - 1) / 2)) {
                // 왼쪽 형제 노드가 있는 경우
                if (i > 0) {
                    LeafNode* left_sibling = internal->children[i-1]->as_leaf();
                    // 왼쪽 형제 노드에서 키를 빌릴 수 있는지 확인
                    if (left_sibling->keys.size() > static_cast<size_t>((degree - 1) / 2)) {
                        // 왼쪽 형제 노드의 마지막 키를 가져옴
                        child->keys.insert(child->keys.begin(), left_sibling->keys.back());
                        left_sibling->keys.pop_back();
                        // 구분자 키 업데이트
                        internal->keys[i - 1] = child->keys[0];
                        return true;
                    }   
                }

                // 오른쪽 형제 노드가 있는 경우
                if (i < internal->children.size() - 1) {
                    LeafNode* right_sibling = internal->children[i+1]->as_leaf();
                    // 오른쪽 형제 노드에서 키를 빌릴 수 있는지 확인
                    if (right_sibling->keys.size() > static_cast<size_t>((degree - 1) / 2)) {
                        // 오른쪽 형제 노드의 첫 번째 키를 가져옴
                        child->keys.push_back(right_sibling->keys.front());
                        right_sibling->keys.erase(right_sibling->keys.begin());
                        // 구분자 키 업데이트
                        internal->keys[i] = right_sibling->keys[0];
                        return true;
                    }
                }

                // 왼쪽 형제 노드와 병합
                if (i > 0) {
                    LeafNode* left_sibling = internal->children[i-1]->as_leaf();
                    // 왼쪽 형제 노드에 현재 노드의 키들을 추가
                    left_sibling->keys.insert(left_sibling->keys.end(), child->keys.begin(), child->keys.end());
                    // 링크드 리스트 연결
                    left_sibling->next = child->next;
                    // Internal node에서 키와 자식 노드 삭제
                    internal->keys.erase(internal->keys.begin() + i - 1);
                    internal->children.erase(internal->children.begin() + i);
                    // 현재 노드 메모리 해제
                    delete child;
                } else {
                    // 오른쪽 형제 노드와 병합
                    LeafNode* right_sibling = internal->children[i+1]->as_leaf();
                    // 현재 노드에 오른쪽 형제 노드의 키들을 추가
                    child->keys.insert(child->keys.end(), right_sibling->keys.begin(), right_sibling->keys.end());
                    // 링크드 리스트 연결
                    child->next = right_sibling->next;
                    // Internal node에서 키와 자식 노드 삭제
                    internal->keys.erase(internal->keys.begin() + i);
                    internal->children.erase(internal->children.begin() + i + 1);
                    // 오른쪽 형제 노드 메모리 해제
                    delete right_sibling;
                }
            }
        }
    }
    return true;
}


// FindLeaf function: Traverses the B+ Tree from the root to find the leaf node that should contain the given key.
template<typename Key>
typename Bplustree<Key>::LeafNode* Bplustree<Key>::FindLeaf(const Key& key) const {
    // root node부터 시작
    Node* current = root;

    // Leaf Node 찾을 때까지 트리를 내려감
    while (!current->is_leaf) {
        // InternalNode로 타입 변환
        InternalNode* internal = current->as_internal();
        size_t i = 0;

        // 적절한 자식 노드 찾기
        // key보다 큰 첫 번째 키를 찾을 때까지 반복
        while (i < internal->keys.size() && key >= internal->keys[i]) {
            i++;
        }

        // 찾은 자식 노드로 이동
        current = internal->children[i];
    }

    // LeafNode로 타입 변환하여 반환
    return current->as_leaf();
}

// Print function: Public interface to print the B+ Tree structure.
template<typename Key>
void Bplustree<Key>::Print() const {
    PrintRecursive(root, 0);
}

// Helper function: Recursively prints the tree structure with indentation based on tree level.
template<typename Key>
void Bplustree<Key>::PrintRecursive(const Node* node, int level) const {
    if (node == nullptr) return;
    // Indent based on the level in the tree.
    for (int i = 0; i < level; ++i)
        std::cout << "  ";
    if (node->is_leaf) {
        // Print leaf node keys.
        const LeafNode* leaf = static_cast<const LeafNode*>(node);
        std::cout << "[Leaf] ";
        for (const Key& key : leaf->keys)
            std::cout << key << " ";
        std::cout << std::endl;
    } else {
        // Print internal node keys and recursively print children.
        const InternalNode* internal = static_cast<const InternalNode*>(node);
        std::cout << "[Internal] ";
        for (const Key& key : internal->keys)
            std::cout << key << " ";
        std::cout << std::endl;
        for (const Node* child : internal->children)
            PrintRecursive(child, level + 1);
    }
}