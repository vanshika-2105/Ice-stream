import { ReactFlow, Background, Controls } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

const nodes = [
  {
    id: "kafka",
    position: { x: 50, y: 150 },
    data: {
      label: (
        <div>
          <strong>Kafka</strong>
          <br />
          <span>Ingest</span>
        </div>
      ),
    },
  },
  {
    id: "flink",
    position: { x: 350, y: 150 },
    data: {
      label: (
        <div>
          <strong>Flink</strong>
          <br />
          <span>Process</span>
        </div>
      ),
    },
  },
  {
    id: "iceberg",
    position: { x: 650, y: 150 },
    data: {
      label: (
        <div>
          <strong>Iceberg</strong>
          <br />
          <span>Serve</span>
        </div>
      ),
    },
  },
];

const edges = [
  {
    id: "kafka-flink",
    source: "kafka",
    target: "flink",
    animated: true,
  },
  {
    id: "flink-iceberg",
    source: "flink",
    target: "iceberg",
    animated: true,
  },
];

function Pipeline() {
  return (
    <section className="pipeline-section">
      <h2>Data Pipeline</h2>

      <div className="pipeline-container">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          fitView
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
        >
          <Background />
          <Controls />
        </ReactFlow>
      </div>
    </section>
  );
}

export default Pipeline;
