import React, { useEffect, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
} from "recharts";

const TickerChart = ({ ticker = "AAPL" }) => {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await fetch(`http://127.0.0.1:8000/ticker/${ticker}`);
        const json = await res.json();

        // json.points = [{ timestamp, close }, ...]
        const chartData = json.points.map((p) => ({
          time: new Date(p.timestamp).toLocaleTimeString("en-US", {
            hour: "2-digit",
            minute: "2-digit",
          }),
          close: p.close,
        }));

        setData(chartData);
      } catch (err) {
        console.error("Error fetching ticker data:", err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [ticker]);

  if (loading) return <div>Loading {ticker} data...</div>;

  return (
    <div style={{ width: "100%", height: 400 }}>
      <h2>{ticker} – Last Day (1m candles)</h2>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="time" minTickGap={20} />
          <YAxis domain={["dataMin", "dataMax"]} />
          <Tooltip />
          <Line type="monotone" dataKey="close" dot={false} strokeWidth={2} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default TickerChart;
