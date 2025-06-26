import mmap, os, struct

class MMapChainedHashTable:
    # ────────────────────────────────────────────────────────────
    # tunables
    TABLE_SIZE      = 100_000
    SLOT_SIZE       = 8
    KEY_SIZE        = 32
    MAX_ITEMS       = 64
    DATA_REGION_MB  = 500

    # ────────────────────────────────────────────────────────────
    # derived constants
    NODE_HEADER_SIZE = KEY_SIZE + 4 + 8 + 8  # key + count + next_same + next_other
    NODE_SIZE        = NODE_HEADER_SIZE + MAX_ITEMS * 4
    DATA_REGION_SIZE = DATA_REGION_MB * 1024 * 1024

    # The free list head will be stored at the very beginning of the data region.
    # This means the actual data for nodes will start after this free list head pointer.
    # We'll use 8 bytes (Q) for the free list head, just like node pointers.
    FREE_LIST_HEAD_SIZE = 8
    # Corrected: Use class attributes directly as they are already defined at this point
    DATA_START_OFFSET = (SLOT_SIZE * TABLE_SIZE) + FREE_LIST_HEAD_SIZE

    # Corrected: Use class attributes directly
    FILE_SIZE        = (SLOT_SIZE * TABLE_SIZE) + FREE_LIST_HEAD_SIZE + DATA_REGION_SIZE

    def __init__(self, filename="mmap_chain_hash.dat"):
        self.filename = filename
        self._init_file()
        self.f  = open(self.filename, "r+b")
        self.mm = mmap.mmap(self.f.fileno(), self.FILE_SIZE)

        # The offset where new nodes are allocated if the free list is empty
        # This starts after the hash table slots and the free list head pointer
        self.next_new_node_offset = self.DATA_START_OFFSET

        # Corrected line: Access SLOT_SIZE and TABLE_SIZE as class attributes
        self.free_list_head = self._read_u64(self.SLOT_SIZE * self.TABLE_SIZE) # Position of the free list head pointer


    def _init_file(self):
        if not os.path.exists(self.filename):
            print(f"Initializing new file: {self.filename} with size {self.FILE_SIZE / (1024*1024):.2f} MB")
            # Step 1: Create the file with the correct size using 'wb' mode
            # 'wb' mode will create the file if it doesn't exist, or truncate it if it does.
            with open(self.filename, "wb") as f_create:
                f_create.truncate(self.FILE_SIZE)

            # Step 2: Now that the file exists and has the correct size,
            # open it in "r+b" mode for mmap.
            f_init = open(self.filename, "r+b")
            try:
                temp_mm = mmap.mmap(f_init.fileno(), self.FILE_SIZE)
                try:
                    # Initialize all slots to 0 (NULL pointer)
                    for i in range(self.TABLE_SIZE):
                        struct.pack_into("Q", temp_mm, i * self.SLOT_SIZE, 0) # Clear hash table slots
                    # Initialize the free list head to 0 (empty)
                    struct.pack_into("Q", temp_mm, self.SLOT_SIZE * self.TABLE_SIZE, 0) # Clear free list head
                    temp_mm.flush()
                finally:
                    temp_mm.close() # Ensure mmap object is closed
            finally:
                f_init.close() # Ensure the file is closed at the end of initialization
        # If the file already exists, nothing to do in this block.
        # The main __init__ will open it and mmap it.
    def _hash(self, key: str) -> int:
        return sum(key.encode("utf-8")) % self.TABLE_SIZE

    def _slot_off(self, index: int) -> int:
        return index * self.SLOT_SIZE

    def _read_u64(self, off: int) -> int:
        return struct.unpack_from("Q", self.mm, off)[0]

    def _write_u64(self, off: int, val: int) -> None:
        struct.pack_into("Q", self.mm, off, val)

    def _read_node(self, off: int):
        # A 0 offset signifies a NULL pointer in our design
        if off == 0:
            return None
        key_bytes = self.mm[off : off + self.KEY_SIZE]
        count     = struct.unpack_from("I", self.mm, off + self.KEY_SIZE)[0]
        same_off  = struct.unpack_from("Q", self.mm, off + self.KEY_SIZE + 4)[0]
        other_off = struct.unpack_from("Q", self.mm, off + self.KEY_SIZE + 12)[0]
        key       = key_bytes.rstrip(b"\x00").decode("utf-8")
        base      = off + self.NODE_HEADER_SIZE
        # Read items. Be careful not to read beyond 'count' elements.
        items     = [struct.unpack_from("I", self.mm, base + i*4)[0] for i in range(count)]
        return {"key": key, "count": count, "same": same_off, "other": other_off, "off": off, "items": items}

    def _write_empty_node(self, off: int, key: str, same: int = 0, other: int = 0):
        # When writing an empty node, ensure all fields are initialized, especially count to 0
        key_b = key.encode("utf-8")[:self.KEY_SIZE].ljust(self.KEY_SIZE, b"\x00")
        struct.pack_into(f"{self.KEY_SIZE}sIQQ", self.mm, off, key_b, 0, same, other)
        # Clear items area just to be safe, though not strictly necessary as count is 0
        # for i in range(self.MAX_ITEMS):
        #     struct.pack_into("I", self.mm, off + self.NODE_HEADER_SIZE + i*4, 0)


    def _append_item_to_node(self, node_off: int, doc_id: int):
        count_off = node_off + self.KEY_SIZE
        count     = struct.unpack_from("I", self.mm, count_off)[0]
        if count >= self.MAX_ITEMS:
            raise ValueError("Node already full")
        items_base = node_off + self.NODE_HEADER_SIZE
        struct.pack_into("I", self.mm, items_base + count*4, doc_id)
        struct.pack_into("I", self.mm, count_off, count + 1)

    def _alloc_node(self) -> int:
        # 1. Try to get a node from the free list
        if self.free_list_head != 0:
            node_off = self.free_list_head
            # The next_same pointer of the freed node points to the next free node
            self.free_list_head = self._read_u64(node_off + self.KEY_SIZE + 4) # Read 'same' pointer of freed node
            # Update the free list head in the file
            # Corrected line: Access SLOT_SIZE and TABLE_SIZE as class attributes
            self._write_u64(self.SLOT_SIZE * self.TABLE_SIZE, self.free_list_head)
            return node_off
        else:
            # 2. If free list is empty, allocate new space
            if self.next_new_node_offset + self.NODE_SIZE > self.FILE_SIZE:
                raise RuntimeError("Out of data region - file is full")
            off = self.next_new_node_offset
            self.next_new_node_offset += self.NODE_SIZE
            return off

    def _free_node(self, node_off: int):
        if node_off == 0: # Cannot free the NULL pointer
            return

        # Link the freed node to the head of the free list
        # Store the current free_list_head in the 'next_same' pointer of the node being freed
        self._write_u64(node_off + self.KEY_SIZE + 4, self.free_list_head)
        # Update the free_list_head to point to the newly freed node
        self.free_list_head = node_off
        # Write the new free list head back to the file
        # Corrected line: Access SLOT_SIZE and TABLE_SIZE as class attributes
        self._write_u64(self.SLOT_SIZE * self.TABLE_SIZE, self.free_list_head)

        # Optionally, clear the node's key and count for debugging/safety
        self._write_empty_node(node_off, "") # Clears key, sets count to 0, same/other to 0

    def insert(self, key: str, doc_ids):
        if isinstance(doc_ids, int): doc_ids = [doc_ids]
        if not doc_ids: return

        bucket     = self._hash(key)
        slot_off   = self._slot_off(bucket)
        node_off   = self._read_u64(slot_off) # Current head of the bucket's chain
        prev_other = 0 # Offset of the node preceding the current 'node_off' in the 'other' chain

        # 1. Traverse 'other' chain to find an existing node for the key or the end of the chain
        current_other_node_off = node_off
        found_key_node_off = 0 # Stores the offset of the first node found for 'key'

        while current_other_node_off != 0:
            node = self._read_node(current_other_node_off)
            if node["key"] == key:
                found_key_node_off = current_other_node_off
                break # Found a node for the key, now check its 'same' chain
            prev_other = current_other_node_off
            current_other_node_off = node["other"]

        if found_key_node_off != 0:
            # Key exists: Traverse 'same' chain to add doc_ids or find a suitable node
            current_same_node_off = found_key_node_off
            prev_same = 0 # Offset of the node preceding the current 'current_same_node_off' in the 'same' chain

            while current_same_node_off != 0 and doc_ids:
                n = self._read_node(current_same_node_off)
                prev_same = current_same_node_off

                # Try to add doc_ids to this node
                # Make a copy of doc_ids to iterate and remove from the original list
                for d in list(doc_ids): # Iterate over a copy to allow modification of doc_ids
                    if n["count"] < self.MAX_ITEMS:
                        # Check if doc_id is already in the node to prevent duplicates
                        if d not in n["items"]:
                            self._append_item_to_node(current_same_node_off, d)
                            doc_ids.remove(d)
                        else:
                            doc_ids.remove(d) # doc_id already present, remove from pending
                    else:
                        break # Node is full, move to the next 'same' node

                current_same_node_off = n["same"]

            # If there are still doc_ids, create new nodes in the 'same' chain
            while doc_ids:
                new_off = self._alloc_node()
                self._write_empty_node(new_off, key) # Initialize new node with key and count=0

                # Append items to the new node
                items_to_add = doc_ids[:self.MAX_ITEMS]
                for d in items_to_add:
                    self._append_item_to_node(new_off, d)
                doc_ids = doc_ids[self.MAX_ITEMS:]

                # Link the new node to the 'same' chain
                if prev_same == 0: # This case shouldn't happen if found_key_node_off was set, but as a safeguard
                    # This would mean the found_key_node_off itself was full and this is the first 'same' overflow
                    # In a robust implementation, this might point to a logic error or a very specific edge case.
                    # For now, it implies the original 'found_key_node_off' should link to this 'new_off'
                    # which is handled implicitly by ensuring prev_same is always updated.
                    # Correct logic should be: if prev_same is the found_key_node_off
                    # then link that node's 'same' pointer to new_off.
                    self._write_u64(found_key_node_off + self.KEY_SIZE + 4, new_off)
                else:
                    self._write_u64(prev_same + self.KEY_SIZE + 4, new_off) # Link previous 'same' to new node
                prev_same = new_off # Update for next iteration

        else:
            # Key does not exist: Create new node(s) and link them to the 'other' chain
            first_new_node_off = 0 # To store the offset of the very first node created for this key
            current_new_node_off = 0 # To track the last node created in the 'same' chain for this key

            while doc_ids:
                new_off = self._alloc_node()
                if first_new_node_off == 0:
                    first_new_node_off = new_off # This is the first node for this key

                # Initialize the new node
                self._write_empty_node(new_off, key)

                # Add items to the new node
                items_to_add = doc_ids[:self.MAX_ITEMS]
                for d in items_to_add:
                    self._append_item_to_node(new_off, d)
                doc_ids = doc_ids[self.MAX_ITEMS:]

                # Link the 'same' chain for these new nodes
                if current_new_node_off != 0: # If this is not the very first node of this new key
                    self._write_u64(current_new_node_off + self.KEY_SIZE + 4, new_off) # Link previous 'same' to current new
                current_new_node_off = new_off # Update for next iteration

            # Now, link the first node of this new key's chain into the 'other' chain
            if prev_other == 0: # This key is the first in its bucket's 'other' chain
                self._write_u64(slot_off, first_new_node_off)
            else: # Link to the last node in the 'other' chain
                self._write_u64(prev_other + self.KEY_SIZE + 12, first_new_node_off) # Update 'other' pointer of previous node

    def get(self, key: str):
        bucket     = self._hash(key)
        node_off   = self._read_u64(self._slot_off(bucket))
        items = []

        while node_off != 0:
            node = self._read_node(node_off)
            if node["key"] == key:
                # Found the key, now collect all items from this 'same' chain
                current_same_node_off = node_off
                while current_same_node_off != 0:
                    current_node = self._read_node(current_same_node_off)
                    items.extend(current_node["items"])
                    current_same_node_off = current_node["same"]
                break # All items for this key collected
            node_off = node["other"] # Move to next node in 'other' chain if key not found
        return items if items else None

    def remove_doc_id(self, key: str, doc_id: int):
        bucket           = self._hash(key)
        slot_off         = self._slot_off(bucket)
        current_node_off = self._read_u64(slot_off)
        prev_other_node_off = 0 # To relink nodes in the 'other' chain

        while current_node_off != 0:
            node = self._read_node(current_node_off)
            if node["key"] == key:
                # Found the key, now traverse the 'same' chain
                current_same_node_off = current_node_off
                prev_same_node_off = 0 # To relink nodes in the 'same' chain

                while current_same_node_off != 0:
                    n = self._read_node(current_same_node_off)
                    items_in_node = n["items"]

                    try:
                        # Find the index of doc_id in this node
                        i = items_in_node.index(doc_id)
                        last_idx = n["count"] - 1
                        items_base = n["off"] + self.NODE_HEADER_SIZE

                        # If the item is not the last one, swap it with the last item
                        if i != last_idx:
                            last_val = items_in_node[last_idx]
                            struct.pack_into("I", self.mm, items_base + i*4, last_val)
                        # Decrement count
                        struct.pack_into("I", self.mm, n["off"] + self.KEY_SIZE, last_idx) # Write new count

                        # Check if the node is now empty and should be freed
                        if last_idx == 0: # Node is now empty (count became 0)
                            # Relink the chain to bypass the node being freed
                            if prev_same_node_off == 0: # This was the first node in the 'same' chain
                                # This node might also be the first node in the 'other' chain from the slot
                                if prev_other_node_off == 0: # This node is directly pointed to by the slot
                                    self._write_u64(slot_off, n["other"]) # Slot now points to the next 'other'
                                else: # This node is pointed to by a previous 'other' node
                                    self._write_u64(prev_other_node_off + self.KEY_SIZE + 12, n["other"]) # Prev 'other' links to next 'other'
                            else: # This node is in the middle or end of the 'same' chain
                                # Link previous 'same' node to the next 'same' node of the current node
                                self._write_u64(prev_same_node_off + self.KEY_SIZE + 4, n["same"])

                            # Free the node
                            self._free_node(current_same_node_off)
                        return # Doc_id removed

                    except ValueError:
                        # doc_id not in this node, move to the next in 'same' chain
                        pass

                    prev_same_node_off = current_same_node_off
                    current_same_node_off = n["same"]
                return # Key found, but doc_id not in any 'same' node for that key

            prev_other_node_off = current_node_off
            current_node_off = node["other"] # Move to next node in 'other' chain

    def close(self):
        self.mm.flush()
        # Persist the free_list_head to the file before closing
        # Corrected line: Access SLOT_SIZE and TABLE_SIZE as class attributes
        self._write_u64(self.SLOT_SIZE * self.TABLE_SIZE, self.free_list_head)
        self.mm.close()
        self.f.close()
        print(f"MMapChainedHashTable closed. Free list head saved at offset {self.SLOT_SIZE * self.TABLE_SIZE}.")
