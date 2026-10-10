 import { ReactFlow, Background, Controls } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

function Pipeline({ status = "UNKNOWN" }) {
  const getStatusSymbol = (nodeName) => {
    if (nodeName === "Quality Engine") {
      if (status === "CRITICAL") return "✕";
      if (status === "WARNING") return "⚠";
      if (status === "HEALTHY") return "✓";
    }

    return "✓";
  };

  const nodes = [
    {
      id: "1",
      position: { x: 50, y: 100 },
      data: {
        label: `Producer ${getStatusSymbol("Producer")}`,
      },
    },
    {
      id: "2",
      position: { x: 250, y: 100 },
      data: {
        label: `Kafka ${getStatusSymbol("Kafka")}`,
      },
    },
    {
      id: "3",
      position: { x: 450, y: 100 },
      data: {
        label: `Flink ${getStatusSymbol("Flink")}`,
      },
    },
    {
      id: "4",
      position: { x: 650, y: 100 },
      data: {
        label: `Quality Engine ${getStatusSymbol("Quality Engine")}`,
      },
    },
    {
      id: "5",
      position: { x: 850, y: 100 },
      data: {
        label: `Iceberg ${getStatusSymbol("Iceberg")}`,
      },
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
    {
      id: "e3-4",
      source: "3",
      target: "4",
      animated: true,
    },
    {
      id: "e4-5",
      source: "4",
      target: "5",
      animated: true,
    },
  ];

  return (
    <section className="pipeline-section">
      <h2>Data Pipeline</h2>

      <div
        className="pipeline-container"
        style={{ height: "300px" }}
      >
        <ReactFlow
          nodes={nodes}
          edges={edges}
          fitView
        >
          <Background />
          <Controls />
        </ReactFlow>
      </div>
    </section>
  );
}

export default Pipeline;
