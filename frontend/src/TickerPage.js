import React, { useEffect, useState } from "react";
import { useParams } from "react-router";
import TickerChart from "./TickerChartComponent";

function TickerPage() {
  const { ticker } = useParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [page, setPage] = useState(1);

  const PAGE_SIZE = 10; // you can change this

  useEffect(() => {
    // reset to first page when ticker changes
    setPage(1);
  }, [ticker]);

  useEffect(() => {
    const fetchNews = async () => {
      try {
        setLoading(true);
        setError("");

        const res = await fetch(
          `http://127.0.0.1:8000/news?ticker=${ticker}&page=${page}&page_size=${PAGE_SIZE}`
        );

        if (!res.ok) {
          throw new Error(`Failed to fetch news (${res.status})`);
        }

        const json = await res.json();
        setData(json);
      } catch (err) {
        setError(err.message || "Something went wrong");
      } finally {
        setLoading(false);
      }
    };

    if (ticker) {
      fetchNews();
    }
  }, [ticker, page]);

  const hasPrev = page > 1;
  const hasNext =
    data && data.total != null
      ? page * PAGE_SIZE < data.total
      : data?.items?.length === PAGE_SIZE; // fallback

  return (
    <div style={{ maxWidth: 900, margin: "0 auto", padding: "24px" }}>
      <h1>Live Price</h1>
      <TickerChart ticker={ticker} />
      <hr />
      <h1 style={{ fontSize: "24px", marginBottom: "8px" }}>{ticker} News</h1>
      <p style={{ color: "#555", marginBottom: "24px" }}>
        Latest news articles for <strong>{ticker}</strong>.
      </p>

      {loading && <div>Loading news…</div>}
      {error && (
        <div style={{ color: "red", marginBottom: "16px" }}>{error}</div>
      )}

      {!loading && !error && data && data.items && data.items.length === 0 && (
        <div>No news found for this ticker.</div>
      )}

      {/* News cards */}
      {!loading && !error && data && data.items && data.items.length > 0 && (
        <>
          <div
            style={{ display: "flex", flexDirection: "column", gap: "16px" }}
          >
            {data.items.map((item) => (
              <div
                key={item.id}
                style={{
                  border: "1px solid #e0e0e0",
                  borderRadius: "8px",
                  padding: "16px",
                  boxShadow: "0 1px 3px rgba(0,0,0,0.05)",
                }}
              >
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    marginBottom: "4px",
                  }}
                >
                  <span style={{ fontSize: "12px", color: "#777" }}>
                    {item.source}
                  </span>
                  <span style={{ fontSize: "12px", color: "#777" }}>
                    {item.updated_time}
                  </span>
                </div>
                <a
                  href={item.canonical_url}
                  target="_blank"
                  rel="noreferrer"
                  style={{
                    fontSize: "18px",
                    fontWeight: 600,
                    textDecoration: "none",
                    color: "#1a73e8",
                    display: "block",
                    marginBottom: "8px",
                  }}
                >
                  {item.title}
                </a>
                <p style={{ margin: 0, fontSize: "14px", color: "#333" }}>
                  {item.description}
                </p>
              </div>
            ))}
          </div>

          {/* Pagination arrows */}
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginTop: "24px",
            }}
          >
            <button
              onClick={() => hasPrev && setPage((p) => p - 1)}
              disabled={!hasPrev}
              style={{
                border: "none",
                background: "transparent",
                cursor: hasPrev ? "pointer" : "default",
                opacity: hasPrev ? 1 : 0.4,
                fontSize: "14px",
              }}
            >
              ← Previous
            </button>

            <span style={{ fontSize: "14px", color: "#555" }}>
              Page {page}
              {data?.total != null
                ? ` of ${Math.max(1, Math.ceil(data.total / PAGE_SIZE))}`
                : ""}
            </span>

            <button
              onClick={() => hasNext && setPage((p) => p + 1)}
              disabled={!hasNext}
              style={{
                border: "none",
                background: "transparent",
                cursor: hasNext ? "pointer" : "default",
                opacity: hasNext ? 1 : 0.4,
                fontSize: "14px",
              }}
            >
              Next →
            </button>
          </div>
        </>
      )}
    </div>
  );
}

export default TickerPage;