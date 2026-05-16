use crate::lattice::LatticeCoordinate;

pub fn compress_coordinates(coords: &[LatticeCoordinate]) -> Vec<i64> {
    coords
        .iter()
        .flat_map(|c| [c.x, c.y, c.z])
        .collect::<Vec<_>>()
}
