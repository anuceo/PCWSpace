use rayon::prelude::*;

pub fn apply_parallel(items: &[String]) -> Vec<String> {
    items
        .par_iter()
        .map(|item| format!("replayed:{item}"))
        .collect::<Vec<_>>()
}
