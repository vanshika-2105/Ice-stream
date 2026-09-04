  import { ReactFlow, Background, Controls } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

const nodes= [
  {
    id: "1",
    position: { x: 50, y: 100 },
    data: { label: "Kafka\nIngest" },
  },
  {
    id: "2",
    position: { x: 300, y: 100 },
    data: { label: "Flink\nProcess" },
  },
  {
    id: "3",
    position: { x: 550, y: 100 },
    data: { label: "Iceberg\nServe" },
  },
];

const edges = [
  {
    id: "e1-2",
    source: "1",
    target: "2",
    animated: true,
  },
  {
    id: "e2-3",
    source: "2",
    target: "3",
    animated: true,
  },
];

function Pipeline() {
  return (
    <section className="pipeline-section">
      <h2>Data Pipeline</h2>

      <div className="pipeline-container">
        <ReactFlow nodes={nodes} edges={edges} fitView>
          <Background />
          <Controls />
        </ReactFlow>
      </div>
    </section>
  );
}

export default Pipeline;
