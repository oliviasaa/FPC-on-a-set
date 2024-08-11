use crate::aux_types::{NodeId, TxId, Opinion, NodeStatus, HashedTxId, TxGlobalStatus, Neighborhood, Vision, Conflicts, TxGraphType, NodeGraphType, intersects};
use crate::constants::{BETA, K, L};
use std::collections::BTreeMap;
use rand_core::{OsRng, RngCore};

// A node is a collection of the following information:
//
// id: random public id (i.e., known to other nodes)
// vision: a collection of known transactions ids, their conflicts and an opinion about it
// neighborhood: a collection of known nodes, to which the node can communicate
// status: represents is the node finalized all its opinions or not
// node_type: malicious, faulty or honest

#[derive(Debug)]
pub struct Node {
    pub id: NodeId,
    pub vision: Vision,
    pub neighborhood: Neighborhood,
    pub status: NodeStatus,
    pub node_type: NodeType
} 

impl Node {
    // Samples k nodes without repetition from the neighborhood
    fn sample_from_neighborhood(&self, k: usize) -> Vec<NodeId> {
        self.neighborhood.sample(k)
    }

    fn collect_and_set_new_opinion(&self, k: usize, database: &Database, random_number: u32) -> Vec<(TxId, bool)> {
        let node_sample = self.sample_from_neighborhood(k);

        let mut eta = self.vision.get_txs().iter()
            .map(|id| (*id, 0usize) )
            .collect::<Vec<(TxId, usize)>>();

        for queried_node in node_sample {
            if database.data.get(&queried_node).unwrap().is_honest(){
                for (tx, likes) in &mut eta {
                    let opinion = database.data
                        .get(&queried_node).unwrap()
                        .vision
                        .get_opinion_status(tx); 
                    if opinion.is_like() {
                        *likes += 1;
                    }
                }
            } else if database.data.get(&queried_node).unwrap().is_malicious(){
                for (tx, likes) in &mut eta {
                    let opinion = database.data
                        .get(&self.id).unwrap()
                        .vision
                        .get_opinion_status(tx); 
                    if opinion.is_like() {
                        *likes += 1;
                    }
                }
            }
        }

        let number_of_queries = k.min(self.neighborhood.len());
        let threshold = ((random_number as u128 * number_of_queries as u128)/(u32::max_value() as u128)) as usize;
        let mut new_auxiliary_opinion = Vec::new();
        for (id, n) in eta {
            if n > threshold { 
                new_auxiliary_opinion.push((id, true)); 
            } else {
                new_auxiliary_opinion.push((id, false));
            }
        }

        self.elim(&mut new_auxiliary_opinion, random_number);
        self.comp(new_auxiliary_opinion, random_number)

    }


    // 'elim' step of the algorithm: given an auliliary opinion, 
    // it orders the txs accordingly to the random number sent by the oracle 
    // and uses this order to eliminate transactions from the liked set until the set is independent.
    fn elim(&self, auxiliary_opinion: &mut Vec<(TxId, bool)>, random_number: u32){
        // Sorts auliliary opinion vector by hashed TxId (largest to smallest)
        auxiliary_opinion.sort_by(move |a, b| {
            let hash_a = HashedTxId {
                id: a.0,
                random_number
            };
            let hash_b = HashedTxId {
                id: b.0,
                random_number
            };
            hash_b.cmp(&hash_a)
        });

        // holds the set of liked txs, for later use
        let mut liked_set = auxiliary_opinion.iter()
            .filter(|(_, b)| *b )
            .map(|(a,_)| a)
            .collect::<Vec<TxId>>();

        // For each liked tx, stops liking it (and removes it from the liked_set) 
        // if it conflicts with something else in the liked_set
        // This is done in the order introduced above (sorted by hashed TxId)
        for (txid, opinion) in auxiliary_opinion.iter_mut() {
            if *opinion {

                // nodes will never try to access an unknown tx by design
                let conflicts = self.vision.get_conflict_set(&txid); 

                if intersects(conflicts.get(), &liked_set) {
                    *opinion = false;
                    liked_set.retain(|&x| x!= *txid);
                }   
            }
        }
    }

    // 'comp' step of the algorithm: given an auliliary opinion after the 'elim' step,
    // which means that now the liked set is independent, 
    // it orders the txs accordingly to the random number sent by the oracle 
    // and uses this order to add transactions from the unliked set until the liked set is maximal.
    fn comp(&self, mut auxiliary_opinion: Vec<(TxId, bool)>, random_number: u32) -> Vec<(TxId, bool)> {
        // Sorts auliliary opinion vector by hashed TxId (smallest to largest)
        auxiliary_opinion.sort_by(move |a, b| {
            let hash_a = HashedTxId {
                id: a.0,
                random_number
            };
            let hash_b = HashedTxId {
                id: b.0,
                random_number
            };
            hash_a.cmp(&hash_b)
        });

        // holds the set of liked txs, for later use
        let mut liked_set = auxiliary_opinion.iter()
            .filter(|(_, b)| *b )
            .map(|(a,_)| a)
            .collect::<Vec<TxId>>();

        // For each unliked tx, likes it (and adds it to the liked_set) 
        // if it does not conflict with something else in the liked_set
        // This is done in the order introduced above (sorted by hashed TxId)
        for (txid, opinion) in auxiliary_opinion.iter_mut() {
            if !*opinion {
                // nodes will never try to access an unknown tx by design
                let conflicts = self.vision.get_conflict_set(&txid); 

                if !intersects(conflicts.get(), &liked_set) {
                    *opinion = true;
                    liked_set.push(*txid);
                }   
            }
        }

        auxiliary_opinion
    }

    fn update_opinions(&mut self, new_opinions: Vec<(TxId, bool)>){
        for (id, new_opinion) in new_opinions{
            let conflicts = self.vision.get_conflict_set(&id).to_owned();
            let opinion = self.vision.get_opinion_status(&id).to_owned();
            match opinion {
                Opinion::Pending(a, b) if a && new_opinion && b >= L-1 => { 
                    self.vision.set_opinion(&id, Opinion::Final(true));
                    for conflict in conflicts.iter() {
                        self.vision.set_opinion(conflict, Opinion::Final(false));
                    } 
                },
                Opinion::Pending(a, b) if a == new_opinion => { 
                    self.vision.set_opinion(&id, Opinion::Pending(a, b+1));
                },
                Opinion::Pending(a, _) if a != new_opinion => { 
                    self.vision.set_opinion(&id, Opinion::Pending(new_opinion, 0));
                },
                _ => {},
            }
        }

        if self.vision.has_finalized() { 
            self.status = NodeStatus::Finalized; 
        }
    }

    fn is_faulty(&self) -> bool{
        self.node_type == NodeType::Faulty
    }

    fn is_honest(&self) -> bool{
        self.node_type == NodeType::Regular
    }

    fn is_malicious(&self) -> bool{
        self.node_type == NodeType::Malicious
    }
}

#[derive(Debug)]
pub struct Database{
    pub data: BTreeMap<NodeId, Node>, 
    tx_set: Vec<(TxId, TxGlobalStatus)>,
    pub node_set: Vec<(NodeId, NodeType, NodeStatus)>
}

impl Database {
    pub fn generate_new (
        total_node_count: usize, 
        faulty_node_count: usize, 
        malicious_node_count: usize, 
        node_graph_type: NodeGraphType, 
        tx_count: usize, 
        tx_graph_type: TxGraphType,
        initial_distribution: LikeDistributions ) -> Database {
        if malicious_node_count + faulty_node_count >= total_node_count {
            panic!("You need at least 1 honest node");
        }
        
        let (tx_set, common_preliminary_vision) = 
            match tx_graph_type {
                TxGraphType::Complete => generate_complete_conflict_graph(tx_count),
                TxGraphType::Star => generate_star_conflict_graph(tx_count)
            };

        let mut database = Database{
            data: BTreeMap::new(),
            tx_set,
            node_set: Vec::new()
        };

        let honest_node_count = total_node_count - faulty_node_count - malicious_node_count;
        for _ in 0..honest_node_count {
            database.add_new_node(&common_preliminary_vision, node_graph_type, NodeType::Regular);
        }
        
        for _ in 0..faulty_node_count {
            database.add_new_node(&common_preliminary_vision, node_graph_type, NodeType::Faulty);
        }        
        
        for _ in 0..malicious_node_count {
            database.add_new_node(&common_preliminary_vision, node_graph_type, NodeType::Malicious);
        }

        let liked_tx_count = match initial_distribution {
            LikeDistributions::Equal => tx_count,
            LikeDistributions::Concentrated(n) => n
        };

        let n = honest_node_count/liked_tx_count;
        let mut likes = vec![n; liked_tx_count];
        let remaining_likes = honest_node_count - n*liked_tx_count;
        for i in 0..remaining_likes {
            likes[i] += 1;
        }

        let like_proportions = database.tx_set.iter()
            .map(|(a,_)|*a)
            .zip(likes)
            .collect::<Vec<(TxId, usize)>>();

        database.initialize_opinions(like_proportions);

        database
    }


    fn initialize_opinions(&mut self, like_proportions: Vec<(TxId, usize)>){
        let expanded_like_proportions = like_proportions.into_iter()
            .map(|(id, size)| vec![id; size])
            .flatten()
            .collect::<Vec<TxId>>();

        self.data
            .values_mut()
            .filter(|node| node.is_honest() )
            .zip(expanded_like_proportions)
            .for_each( |(node, id)| {
                node.vision.set_opinion(&id, Opinion::Pending(true, 0)); 
            });

        for node in self.data.values_mut().filter(|node| node.is_honest() ) {
            let liked_set = node.vision.get_txs();
            let mut liked_set = liked_set.into_iter().filter(|tx| node.vision.get_opinion(*tx)).collect::<Vec<TxId>>();

            let unset_opinions = node.vision.get_txs().iter()
                .filter( |id| node.vision.get_opinion_status(id).is_none() )
                .collect::<Vec<TxId>>();
            for txid in unset_opinions {
                let conflicts = node.vision.get_conflict_set(&txid); 
                if !intersects(conflicts.get(), &liked_set) {
                    node.vision.set_opinion(&txid, Opinion::Pending(true, 0));
                    liked_set.push(txid);
                } else {
                    node.vision.set_opinion(&txid, Opinion::Pending(false, 0));
                }
            }

        }
    }



    fn _add_new_tx(&mut self, _tx_graph_type: TxGraphType){}

    fn add_new_node(&mut self, vision: &Vision, _node_graph_type: NodeGraphType, node_type: NodeType){
        let new_node_id = NodeId::generate();
        let node_id_set = self.node_set.iter()
            .map(|(id, _, _)| *id)
            .collect::<Vec<NodeId>>();
        let neighborhood = Neighborhood::set_new(&node_id_set);
        for node_id in &node_id_set {
            self.data.get_mut(node_id).unwrap().neighborhood.add(new_node_id);
        }
        self.data.insert(new_node_id, Node{ id: new_node_id, vision: vision.clone() , neighborhood, status: NodeStatus::NotFinalized, node_type: node_type.clone() });
        self.node_set.push((new_node_id, node_type, NodeStatus::NotFinalized));
    }

    pub fn run_fpcs_round(&mut self){

        let random_interval_length = 1.0 - 2.0*BETA;
        let random_number = OsRng.next_u32();
        let random_number = (random_number as f64 * random_interval_length + u32::max_value() as f64 * BETA).floor() as u32;
        let honest_nodes = self.node_set.iter()
            .filter(|(_, node_type, _)| *node_type == NodeType::Regular)
            .map(|(id,_,_)| id)
            .collect::<Vec<NodeId>>();
        let unfinalized_honest_nodes = self.node_set.iter()
            .filter(|(_, node_type, status)| !status.finalized()&& *node_type == NodeType::Regular)
            .map(|(id,_,_)| id)
            .collect::<Vec<NodeId>>();

        for node_id in &unfinalized_honest_nodes {
            let new_opinions = self.data
                .get(node_id).unwrap()
                .collect_and_set_new_opinion(K, self, random_number);
            let node = self.data.get_mut(node_id).unwrap();
            node.update_opinions(new_opinions);
        }

        for (txid, status) in self.tx_set.iter_mut()
            .filter(|(_,status)| !status.finalized()) {
            
            let mut new_status = TxGlobalStatus::Finalized;
            for node_id in &honest_nodes {
                let opinion = self.data.get(node_id).unwrap().vision.get_opinion_status(txid); 
                if !opinion.is_final() {
                    new_status = TxGlobalStatus::NotFinalized; 
                    break; 
                }
            }
            *status = new_status;

            if new_status == TxGlobalStatus::Finalized {
                println!("{:?} finalized in all honest nodes", txid );
                let likes = self.data
                    .values()
                    .map(|node| node.vision.get_opinion(*txid))
                    .filter(|like| *like)
                    .count(); 

                let agreement_rate = (likes.max(honest_nodes.len()-likes) as f64)/(honest_nodes.len() as f64);
                println!("Agreement rate: {:?}", agreement_rate );
            }
        }

        for (id, node_type , status) in self.node_set.iter_mut() {
            if *node_type == NodeType::Regular {
                let old_status = status.clone();
                *status = self.data.get(id).unwrap().status;
                if !old_status.finalized() && status.finalized() {
                    println!("{:?} finalized all transactions", id);
                }
            }
        }

    }

    pub fn is_final(&self) -> bool {
        for (_, node_type, status) in &self.node_set {
            if !status.finalized() && *node_type == NodeType::Regular { return false; }
        } 
        true
    }

    pub fn print_results(&self) {
        for node in self.data.values(){
            if node.is_honest(){
                let txs = node.vision.get_txs();
                let opinion = txs.iter()
                    .map(|tx| (*tx, node.vision.get_opinion_status(tx).clone() ) )
                    .collect::<Vec<(TxId, Opinion)>>(); 
                println!("{:?}: Status {:?}", node.id, node.status);
                println!("Current vision: {:?}", opinion);
            }
        }
    }

}



fn generate_complete_conflict_graph(tx_count: usize) -> (Vec<(TxId, TxGlobalStatus)>, Vision) {

    let tx_id_set = (0..tx_count).map(|_| TxId::generate() ).collect::<Vec<TxId>>();
    let tx_set = tx_id_set.clone().into_iter()
        .zip( vec![TxGlobalStatus::NotFinalized; tx_count] )
        .collect::<Vec<(TxId, TxGlobalStatus)>>();

    let mut common_preliminary_vision = BTreeMap::new();
    for i in 0..tx_count {
        let mut conflicts = tx_id_set.clone();
        let element = conflicts.remove(i);
        let conflict_set = Conflicts::new_from(&conflicts);
        common_preliminary_vision.insert(element, (conflict_set, Opinion::None));
    }

    let common_preliminary_vision = Vision::new_from(&common_preliminary_vision);

    (tx_set, common_preliminary_vision)
}

fn generate_star_conflict_graph(tx_count: usize) -> (Vec<(TxId, TxGlobalStatus)>, Vision) {

    let tx_id_set = (0..tx_count).map(|_| TxId::generate() ).collect::<Vec<TxId>>();
    let tx_set = tx_id_set.clone().into_iter()
        .zip( vec![TxGlobalStatus::NotFinalized; tx_count] )
        .collect::<Vec<(TxId, TxGlobalStatus)>>();

    let mut common_preliminary_vision = BTreeMap::new();
    let center = tx_id_set[0];
    let mut leaves = tx_id_set.clone();
    leaves.remove(0);
    let leaves = Conflicts::new_from(&leaves);

    common_preliminary_vision.insert(center, (leaves, Opinion::None));
    let center = Conflicts::new_from(&vec![center]);

    for i in 1..tx_count {
        common_preliminary_vision.insert(tx_id_set[i], (center.clone(), Opinion::None));
    }

    let common_preliminary_vision = Vision::new_from(&common_preliminary_vision);

    (tx_set, common_preliminary_vision)
}

pub enum LikeDistributions{
    Equal,
    Concentrated(usize)
}


#[derive(Debug, PartialEq, Clone)]
pub enum NodeType {
    Malicious, 
    Faulty,
    Regular
}