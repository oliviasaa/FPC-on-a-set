use std::collections::BTreeMap;
use std::cmp::Ordering;
use std::hash::{DefaultHasher, Hash, Hasher};
use rand_core::{OsRng, RngCore};

#[derive(Debug, Clone)]
pub struct Conflicts(Vec<TxId>);

pub struct ConflictsIterator<'a> {
    conflicts: &'a Conflicts,
    index: usize,
}

impl<'a> Iterator for ConflictsIterator<'a> {
    type Item = &'a TxId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.conflicts.0.len() {
            let result = Some(&self.conflicts.0[self.index]);
            self.index += 1;
            result
        } else {
            None
        }
    }
}

impl Conflicts {
    pub fn get(&self) -> &Vec<TxId> {
        &self.0
    }
    pub fn add(&mut self, tx: TxId) {
        self.0.push(tx);
    }
    pub fn new() -> Self {
        Self(Vec::new())
    }
    pub fn new_from(txs: &Vec<TxId>) -> Self {
        Self(txs.clone())
    }
    pub fn iter(&self) -> ConflictsIterator {
        ConflictsIterator {
            conflicts: self,
            index: 0,
        }
    }
    
}

#[derive(Debug)]
pub struct Neighborhood(Vec<NodeId>);

impl Neighborhood {
    pub fn get(&self) -> &Vec<NodeId> {
        &self.0
    }
    pub fn add(&mut self, node: NodeId) {
        self.0.push(node);
    }
    pub fn new() -> Self {
        Self(Vec::new())
    }
    pub fn set_new(nodes: &Vec<NodeId>) -> Self {
        Self(nodes.clone())
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn iter(&self) -> NeighborhoodIterator {
        NeighborhoodIterator {
            neighborhood: self,
            index: 0,
        }
    }
    // Samples k nodes from the neighborhood and returns them as a vector of ids.
    // if k is larger than the neighborhood size, it returns all the neighborhood.
    // The sampling is done without repetition
    pub fn sample(&self, k: usize) -> Vec<NodeId> {
        let neighborhood = self.get().clone();
        let neighborhood_size = neighborhood.len();

        if k >= neighborhood_size {
            return neighborhood;
        }

        let mut sample = Vec::new();        
        while sample.len() < k {
            let r = OsRng.next_u64();

            let index = ((r as u128 * neighborhood_size as u128)/(u64::max_value() as u128)) as usize;
            if !&sample.contains(&neighborhood[index]) {
                sample.push(neighborhood[index]);
            }
        }

        sample
    }



}

struct NeighborhoodIterator<'a> {
    neighborhood: &'a Neighborhood,
    index: usize,
}

impl<'a> Iterator for NeighborhoodIterator<'a> {
    type Item = &'a NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.neighborhood.0.len() {
            let result = Some(&self.neighborhood.0[self.index]);
            self.index += 1;
            result
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct Vision(BTreeMap<TxId, (Conflicts, Opinion)>);

impl Vision {

    pub fn has_finalized(&self) -> bool {
        let unfinalized_txs = self.0
            .values().cloned()  
            .filter(|(_, opinion)| !opinion.is_final() )
            .collect::<Vec<(Conflicts, Opinion)>>();

        unfinalized_txs.len() == 0
    }

    pub fn get_conflict_set(&self, tx: &TxId) -> &Conflicts {
        // nodes will never try to access an unknown tx by design
        let (conflict_set, _) = self.0
            .get(&tx).expect("unknown TxId"); 

        conflict_set
    }

    pub fn get_txs(&self) -> Vec<TxId> {
        self.0
            .keys()
            .collect()
    }

    pub fn get_opinion(&self, tx: TxId) -> bool {
        // nodes will never try to access an unknown tx by design
        let (_, opinion) = self.0
            .get(&tx).expect("unknown TxId"); 

        opinion.is_like()
    }

    pub fn get_mut_opinion(&mut self, tx: &TxId) -> &mut Opinion {
        // nodes will never try to access an unknown tx by design
        let (_, opinion) = self.0
            .get_mut(&tx).expect("unknown TxId"); 

        opinion
    }

    pub fn get_opinion_status(&self, tx: &TxId) -> &Opinion {
        // nodes will never try to access an unknown tx by design
        let (_, opinion) = self.0
            .get(&tx).expect("unknown TxId"); 

        opinion
    }   

    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn new_from(vision: &BTreeMap<TxId, (Conflicts, Opinion)>) -> Self {
        Self(vision.clone())
    }

    pub fn set_opinion(&mut self, tx: &TxId, new_opinion: Opinion) {
        // nodes will never try to access an unknown tx by design
        let old_opinion = self.get_mut_opinion(&tx); 
        *old_opinion = new_opinion;
    }

}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum Opinion{
    None,
    Pending(bool, u32),
    Final(bool)
}

impl Opinion {
    pub fn is_like(&self) -> bool {
        match *self {
            Self::Final(like) => like,
            Self::Pending(like, _) => like,
            Self::None => false
        }
    }

    pub fn is_none(&self) -> bool {
        match *self {
            Self::None => true,
            _ => false
        }        
    }

    pub fn is_final(&self) -> bool {
        match *self {
            Self::Final(_) => true,
            _ => false 
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NodeGraphType{
    Complete
}

#[derive(Debug, Clone, Copy)]
pub enum TxGraphType{
    Complete,
    Star
}

pub fn intersects(vec1: &Vec<TxId>, vec2: &Vec<TxId>) -> bool {
    for id in vec1 {
        if (*vec2).contains(id) { return true }
    }
    false
}

#[derive(Clone, PartialEq, Debug, Copy)]
pub struct HashedTxId {
    pub id: TxId,
    pub random_number: u32,
}

impl Ord for HashedTxId {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut s = DefaultHasher::new();
        self.id.get_u32().hash(&mut s);
        self.random_number.hash(&mut s);
        let self_hashed_id = s.finish();

        let mut s = DefaultHasher::new();
        other.id.get_u32().hash(&mut s);
        other.random_number.hash(&mut s);
        let other_hashed_id = s.finish();

        self_hashed_id.cmp(&other_hashed_id)
    }
}

impl PartialOrd for HashedTxId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for HashedTxId {}

#[derive(Debug, Clone, Copy)]
pub enum NodeStatus{
    NotFinalized,
    Finalized,
}

impl NodeStatus {
    pub fn finalized(&self) -> bool {
        matches!(*self, Self::Finalized)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TxGlobalStatus{
    NotFinalized,
    Finalized,
}

impl TxGlobalStatus {
    pub fn finalized(&self) -> bool {
        matches!(*self, Self::Finalized)
    }
}

#[derive(Clone, PartialEq, Debug, Eq, Copy, PartialOrd, Ord)]
pub struct TxId(u32);

impl TxId {
    pub fn generate() -> TxId {
        TxId(OsRng.next_u32())
    }
    
    pub fn get_u32(&self) -> u32 {
        self.0
    }

    pub fn from_u32(n: u32) -> TxId {
        TxId(n)
    }
}

impl<'a> FromIterator<&'a TxId> for Vec<TxId>{
    fn from_iter<T: IntoIterator<Item = &'a TxId>>(iter: T) -> Self {
        let mut vec = Vec::new();
        vec.extend(iter);
        vec        
    }
}



#[derive(Clone, PartialEq, Debug, Ord, PartialOrd, Eq, Copy)]
pub struct NodeId(u32);

impl NodeId {
    pub fn generate() -> NodeId {
        NodeId(OsRng.next_u32())
    }
}

impl<'a> FromIterator<&'a NodeId> for Vec<NodeId>{
    fn from_iter<T: IntoIterator<Item = &'a NodeId>>(iter: T) -> Self {
        let mut vec = Vec::new();
        vec.extend(iter);
        vec        
    }
}