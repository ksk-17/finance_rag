import React, { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router";
import { Container, Typography } from "@mui/material"
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TableSortLabel,
  Paper,
  Box,
  Chip,
} from "@mui/material";
import SearchField from "./SearchField";

function fmtNumber(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(x);
}

function fmtPrice(x, currency) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const symbol = currency === "USD" || !currency ? "$" : "";
  return (
    symbol +
    Intl.NumberFormat(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 4,
    }).format(x)
  );
}

function fmtPct(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return (x >= 0 ? "+" : "") + x.toFixed(2) + "%";
}

function fmtCompact(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return Intl.NumberFormat(undefined, {
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(x);
}

function classForChange(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "text-zinc-500";
  return x >= 0 ? "text-emerald-600" : "text-red-600";
}

// Tiny inline sparkline component using SVG (no external libs)
function Sparkline({
  data = [],
  width = 120,
  height = 28,
  strokeWidth = 2,
  up = true,
}) {
  if (!data || data.length < 2) {
    return <div className="text-xs text-zinc-400">no data</div>;
  }
  // Normalize values to [0,1]
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const points = data.map((v, i) => [
    (i / (data.length - 1)) * (width - strokeWidth) + strokeWidth / 2,
    height - ((v - min) / range) * (height - strokeWidth) - strokeWidth / 2,
  ]);
  const path = points
    .map(([x, y], i) => (i === 0 ? `M ${x},${y}` : `L ${x},${y}`))
    .join(" ");
  const color = up ? "#059669" : "#dc2626"; // emerald-600 / red-600

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className="overflow-visible"
    >
      <path
        d={path}
        fill="none"
        stroke={color}
        strokeWidth={strokeWidth}
        strokeLinecap="round"
      />
    </svg>
  );
}

function HomeComponent({ dataUrl = "/api/sp100_table" }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [query, setQuery] = useState("");
  const [sortBy, setSortBy] = useState({ key: "ticker", dir: "asc" });

  const navigate = useNavigate();

  useEffect(() => {
    let active = true;
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch(dataUrl, {
          method: "GET",
          headers: {
            "Content-Type": "application/json",
          },
        });
        if (!response.ok) {
          throw new Error(
            `Failed to load data from ${dataUrl}: ${response.status}`
          );
        }
        const data = await response.json();
        if (!active) return;

        const normalized = (data["sp100"] || []).map((d) => ({
          ...d,
          price: d.price == null ? null : Number(d.price),
          change: d.change == null ? null : Number(d.change),
          change_pct: d.change_pct == null ? null : Number(d.change_pct),
          volume: d.volume == null ? null : Number(d.volume),
          market_cap: d.market_cap == null ? null : Number(d.market_cap),
          sparkline: Array.isArray(d.sparkline) ? d.sparkline.map(Number) : [],
        }));

        setRows(normalized);
      } catch (e) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    return () => {
      active = false;
    };
  }, [dataUrl]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const arr = q
      ? rows.filter(
          (r) =>
            (r.ticker || "").toLowerCase().includes(q) ||
            (r.name || "").toLowerCase().includes(q)
        )
      : rows;
    // sort
    const { key, dir } = sortBy;
    const mul = dir === "asc" ? 1 : -1;
    return [...arr].sort((a, b) => {
      const va = a[key];
      const vb = b[key];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === "number" && typeof vb === "number")
        return (va - vb) * mul;
      return String(va).localeCompare(String(vb)) * mul;
    });
  }, [rows, query, sortBy]);

  function toggleSort(key) {
    setSortBy((s) =>
      s.key === key
        ? { key, dir: s.dir === "asc" ? "desc" : "asc" }
        : { key, dir: "asc" }
    );
  }

  return (
    <Container>
      <div className="p-4 md:p-6">
        <div className="mb-4 flex items-center justify-between gap-2">
          <Typography variant="h5" gutterBottom>
            S&P 100 Dashboard
          </Typography>
          <SearchField
            value={query}
            onChange={setQuery}
            placeholder="Search ticker or name..."
          />
        </div>

        {loading && <div className="text-zinc-600">Loading…</div>}
        {error && (
          <div className="text-red-600 mb-3">Error loading data: {error}</div>
        )}

        {!loading && !error && (
          <TableContainer
            component={Paper}
            variant="outlined"
            sx={{ borderRadius: 3 }}
          >
            <Table size="small">
              <TableHead>
                <TableRow>
                  {[
                    ["ticker", "Ticker"],
                    ["name", "Name"],
                    ["price", "Price"],
                    ["change", "Change"],
                    ["change_pct", "%"],
                    ["volume", "Volume"],
                    ["market_cap", "Mkt Cap"],
                    ["spark", "1D"],
                  ].map(([k, label]) => (
                    <TableCell key={k} sx={{ fontWeight: 600 }}>
                      {k !== "spark" ? (
                        <TableSortLabel
                          active={sortBy.key === k}
                          direction={sortBy.key === k ? sortBy.dir : "asc"}
                          onClick={() => toggleSort(k)}
                        >
                          {label}
                        </TableSortLabel>
                      ) : (
                        label
                      )}
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>

              <TableBody>
                {filtered.map((r) => {
                  const up = (r.change ?? 0) >= 0;
                  return (
                    <TableRow key={r.ticker} hover>
                      <TableCell
                        sx={{ fontWeight: 700, cursor: "pointer" }}
                        onClick={() => navigate(`/${r.ticker}`)}
                      >
                        {r.ticker}
                      </TableCell>
                      <TableCell sx={{ color: "text.secondary" }}>
                        {r.name || "—"}
                      </TableCell>
                      <TableCell>{fmtPrice(r.price, r.currency)}</TableCell>

                      <TableCell
                        sx={{ color: up ? "success.main" : "error.main" }}
                      >
                        {fmtNumber(r.change)}
                      </TableCell>

                      <TableCell>
                        <Chip
                          size="small"
                          variant="outlined"
                          color={up ? "success" : "error"}
                          label={fmtPct(r.change_pct)}
                          sx={{ fontWeight: 600 }}
                        />
                      </TableCell>

                      <TableCell>{fmtCompact(r.volume)}</TableCell>
                      <TableCell>{fmtCompact(r.market_cap)}</TableCell>

                      <TableCell>
                        <Box
                          sx={{ display: "flex", alignItems: "center", gap: 1 }}
                        >
                          <Sparkline data={r.sparkline} up={up} />
                        </Box>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </div>
    </Container>
  );
}

export default HomeComponent;
