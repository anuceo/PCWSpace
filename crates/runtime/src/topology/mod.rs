pub mod deltashot_bolt;
pub mod workflow_bolt;
pub mod workflow_spout;

pub use deltashot_bolt::DeltaShotBolt;
pub use workflow_bolt::WorkflowBolt;
pub use workflow_spout::WorkflowSpout;

use storm::{
    BoltSpec, ComponentSpec, StreamGrouping, Topology,
};

/// Build the PCW Storm topology definition.
///
/// ```text
/// WorkflowSpout ──(shuffle)──> WorkflowBolt
///                ──(shuffle)──> DeltaShotBolt
/// ```
///
/// `binary` is the path to the pcw-storm dispatcher binary that Storm will
/// exec with a component name as argv[1].
pub fn pcw_topology(binary: &str, nimbus_workers: u32) -> Topology {
    let spout_cmd  = vec![binary.to_string(), "workflow-spout".to_string()];
    let wf_bolt    = vec![binary.to_string(), "workflow-bolt".to_string()];
    let ds_bolt    = vec![binary.to_string(), "deltashot-bolt".to_string()];

    Topology::new("pcw")
        .set_config("topology.workers", serde_json::json!(nimbus_workers))
        .set_config("topology.message.timeout.secs", serde_json::json!(60))
        .set_config("topology.max.spout.pending", serde_json::json!(100))
        .add_spout("workflow-spout", ComponentSpec {
            command:     spout_cmd,
            parallelism: 2,
        })
        .add_bolt("workflow-bolt", BoltSpec {
            component: ComponentSpec { command: wf_bolt, parallelism: 4 },
            inputs: vec![(
                "workflow-spout".to_string(),
                StreamGrouping {
                    stream_id: "default".to_string(),
                    grouping:  "shuffle".to_string(),
                    fields:    vec![],
                },
            )],
        })
        .add_bolt("deltashot-bolt", BoltSpec {
            component: ComponentSpec { command: ds_bolt, parallelism: 2 },
            inputs: vec![(
                "workflow-spout".to_string(),
                StreamGrouping {
                    stream_id: "default".to_string(),
                    grouping:  "shuffle".to_string(),
                    fields:    vec![],
                },
            )],
        })
}
