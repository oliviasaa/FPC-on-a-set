use crate::fpcs::{Database, LikeDistributions};
use crate::aux_types::{NodeGraphType, TxGraphType};
use crate::constants::{T, N};

use std::{thread, time};

mod constants;
mod fpcs;
mod aux_types;

fn main(){
        let mut database = Database::generate_new(N, 0, N-2, NodeGraphType::Complete, T, TxGraphType::Star, LikeDistributions::Concentrated(2));
        let mut round = 1;
        while !database.is_final() {
            println!("Round {}", round);    
            database.run_fpcs_round(); 
            //database.print_results();
            thread::sleep(time::Duration::from_millis(1000));
            round += 1;
        }

}

//TODO
// bounds on the constants
