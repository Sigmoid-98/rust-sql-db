use serde::{Deserialize, Serialize};


/// A statement execution plan. The root nodes can perform data modifications or
/// schema changes, in addition to SELECT queries. Beyond the root, the plan is
/// made up of a tree of inner plan nodes that stream and process rows via
/// iterators. Below is an example of an (unoptimized) plan for the given query:
///
/// SELECT title, released, genres.name AS genre
/// FROM movies INNER JOIN genres ON movies.genre_id = genres.id
/// WHERE released >= 2000 ORDER BY released
///
/// Order: movies.released desc
/// └─ Projection: movies.title, movies.released, genres.name as genre
///    └─ Filter: movies.released >= 2000
///       └─ NestedLoopJoin: inner on movies.genre_id = genres.id
///          ├─ Scan: movies
///          └─ Scan: genres
///
/// Rows flow from the tree leaves to the root. The Scan nodes read and emit
/// table rows from storage. They are passed to the NestedLoopJoin node which
/// joins the rows from the two tables, then the Filter node discards old
/// movies, the Projection node picks out the requested columns, and the Order
/// node sorts them before emitting the rows to the client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Plan {
    
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Node {}
