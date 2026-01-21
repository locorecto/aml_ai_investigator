import { useEffect, useMemo, useState } from "react";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "/api/v1";

type CaseSummary = {
  case_id: string;
  party_id?: string | null;
  party_name?: string | null;
  party_type?: string | null;
  risk_rating?: string | null;
  max_risk_score?: number | null;
  alerts_count?: number | null;
  txn_count_case?: number | null;
  amount_total_usd_case?: number | null;
  last_txn_ms_utc_case?: number | null;
  case_window_start_ms_utc?: number | null;
  case_window_end_ms_utc?: number | null;
};

type Pagination = {
  limit: number;
  offset: number;
  total: number;
};

type PaginatedCases = {
  items: CaseSummary[];
  pagination: Pagination;
};

type EvidencePacket = Record<string, unknown>;

type TimelineEntry = {
  case_id: string;
  txn_date_utc?: string | null;
  instrument_type?: string | null;
  txn_count: number;
  amount_total_usd?: number | null;
};

type CopilotSummary = {
  case_summary: string;
  key_indicators: Array<{
    indicator: string;
    evidence_refs?: string[];
    policy_citations?: string[];
  }>;
  benign_explanations_to_rule_out: Array<{
    explanation: string;
    evidence_refs?: string[];
    policy_citations?: string[];
  }>;
  policy_mapping: Array<{
    policy: string;
    citations: string[];
  }>;
  missing_information: string[];
  recommended_disposition: string;
  confidence: number;
  uncertainty_reasons: string[];
  investigator_next_steps: Array<{
    step: string;
    evidence_refs?: string[];
    policy_citations?: string[];
  }>;
  narrative_draft?: string | null;
};

type TableColumn = {
  key: string;
  label: string;
};

const currency = (value?: number | null) => {
  if (value === null || value === undefined) return "n/a";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
};

const dateFromMs = (value?: number | null) => {
  if (!value) return "n/a";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "n/a" : date.toISOString().slice(0, 10);
};

const toLines = (value: string) =>
  value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

const renderTable = (
  rows: Array<Record<string, unknown>>,
  columns: TableColumn[],
  emptyLabel: string
) => {
  if (!rows.length) {
    return <p className="muted">{emptyLabel}</p>;
  }
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col.key}>{col.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={`${row[columns[0]?.key] ?? index}`}>
              {columns.map((col) => (
                <td key={col.key}>{row[col.key] as string}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

async function fetchJson<T>(path: string, options?: RequestInit): Promise<T> {
  const resp = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(detail || `Request failed with ${resp.status}`);
  }
  return resp.json() as Promise<T>;
}

export default function App() {
  const [cases, setCases] = useState<CaseSummary[]>([]);
  const [pagination, setPagination] = useState<Pagination | null>(null);
  const [selectedCase, setSelectedCase] = useState<CaseSummary | null>(null);
  const [packet, setPacket] = useState<EvidencePacket | null>(null);
  const [timeline, setTimeline] = useState<TimelineEntry[]>([]);
  const [copilot, setCopilot] = useState<CopilotSummary | null>(null);
  const [decision, setDecision] = useState("");
  const [narrative, setNarrative] = useState("");
  const [helpful, setHelpful] = useState(true);
  const [wrongParts, setWrongParts] = useState("");
  const [missingData, setMissingData] = useState("");
  const [isLoadingCases, setIsLoadingCases] = useState(false);
  const [isLoadingPacket, setIsLoadingPacket] = useState(false);
  const [isRunningCopilot, setIsRunningCopilot] = useState(false);
  const [feedbackStatus, setFeedbackStatus] = useState("");
  const [error, setError] = useState("");
  const [auth, setAuth] = useState(
    () => localStorage.getItem("aml_auth") === "true"
  );
  const [authInput, setAuthInput] = useState("");

  const totalCases = pagination?.total ?? 0;
  const offset = pagination?.offset ?? 0;
  const limit = pagination?.limit ?? 25;

  const canLoadMore = useMemo(() => offset + limit < totalCases, [offset, limit, totalCases]);

  const loadCases = async (nextOffset = 0) => {
    setIsLoadingCases(true);
    setError("");
    try {
      const payload = await fetchJson<PaginatedCases>(
        `/cases?limit=${limit}&offset=${nextOffset}`
      );
      setCases((prev) =>
        nextOffset === 0 ? payload.items : [...prev, ...payload.items]
      );
      setPagination(payload.pagination);
      if (!selectedCase && payload.items.length > 0) {
        setSelectedCase(payload.items[0]);
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setIsLoadingCases(false);
    }
  };

  const loadCasePacket = async (caseId: string) => {
    setIsLoadingPacket(true);
    setError("");
    try {
      const [packetResp, timelineResp] = await Promise.all([
        fetchJson<EvidencePacket>(`/cases/${caseId}`),
        fetchJson<TimelineEntry[]>(`/cases/${caseId}/timeline`),
      ]);
      setPacket(packetResp);
      setTimeline(timelineResp);
      setCopilot(null);
      setDecision("");
      setNarrative("");
      setFeedbackStatus("");
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setIsLoadingPacket(false);
    }
  };

  const runCopilot = async () => {
    if (!selectedCase) return;
    setIsRunningCopilot(true);
    setError("");
    try {
      const response = await fetchJson<CopilotSummary>(
        `/cases/${selectedCase.case_id}/copilot-summary`,
        { method: "POST" }
      );
      setCopilot(response);
      setDecision(response.recommended_disposition ?? "");
      setNarrative(response.narrative_draft ?? "");
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setIsRunningCopilot(false);
    }
  };

  const submitFeedback = async () => {
    if (!selectedCase) return;
    setFeedbackStatus("Submitting...");
    setError("");
    try {
      await fetchJson(`/cases/${selectedCase.case_id}/feedback`, {
        method: "POST",
        body: JSON.stringify({
          helpful,
          wrong_parts: toLines(wrongParts),
          missing_data: toLines(missingData),
          decision: decision || null,
          narrative: narrative || null,
        }),
      });
      setFeedbackStatus("Feedback saved.");
    } catch (err) {
      setFeedbackStatus("");
      setError((err as Error).message);
    }
  };

  const handleAuth = () => {
    if (authInput.trim() === "investigate") {
      localStorage.setItem("aml_auth", "true");
      setAuth(true);
      setAuthInput("");
    } else {
      setError("Invalid passphrase.");
    }
  };

  useEffect(() => {
    loadCases(0);
  }, []);

  useEffect(() => {
    if (selectedCase?.case_id) {
      loadCasePacket(selectedCase.case_id);
    }
  }, [selectedCase?.case_id]);

  if (!auth) {
    return (
      <div className="login-screen">
        <div className="login-card">
          <h1>AML Investigator Console</h1>
          <p>Dev-only access. Enter the passphrase to continue.</p>
          <input
            type="password"
            value={authInput}
            onChange={(event) => setAuthInput(event.target.value)}
            placeholder="Passphrase"
          />
          <button onClick={handleAuth}>Unlock</button>
        </div>
      </div>
    );
  }

  return (
    <div className="app">
      <header className="app-header">
        <div>
          <h1>AML AI Investigator</h1>
          <p>Evidence-first triage and copilot review.</p>
        </div>
        <div className="header-meta">
          <span>{totalCases} cases</span>
          <span>API: {API_BASE_URL}</span>
        </div>
      </header>

      {error && <div className="banner error">{error}</div>}
      {feedbackStatus && <div className="banner success">{feedbackStatus}</div>}

      <main className="grid">
        <section className="panel panel-left">
          <div className="panel-title">
            <h2>Triage List</h2>
            <button onClick={() => loadCases(0)} disabled={isLoadingCases}>
              Refresh
            </button>
          </div>
          <div className="case-list">
            {cases.map((item) => (
              <button
                key={item.case_id}
                className={`case-row ${
                  selectedCase?.case_id === item.case_id ? "active" : ""
                }`}
                onClick={() => setSelectedCase(item)}
              >
                <div>
                  <strong>{item.party_name || "Unknown party"}</strong>
                  <span>{item.case_id}</span>
                </div>
                <div className="case-metrics">
                  <span>{item.risk_rating || "n/a"}</span>
                  <span>{currency(item.amount_total_usd_case)}</span>
                </div>
              </button>
            ))}
            {isLoadingCases && <p className="muted">Loading cases...</p>}
            {canLoadMore && (
              <button
                className="ghost"
                onClick={() => loadCases(offset + limit)}
                disabled={isLoadingCases}
              >
                Load more
              </button>
            )}
          </div>
        </section>

        <section className="panel panel-center">
          <div className="panel-title">
            <h2>Evidence Packet</h2>
            <span className="muted">
              {selectedCase ? selectedCase.case_id : "No case selected"}
            </span>
          </div>
          {isLoadingPacket && <p className="muted">Loading packet...</p>}
          {packet && (
            <div className="packet">
              <div className="packet-grid">
                <div>
                  <h3>Party Profile</h3>
                  <p>{(packet.party_name as string) ?? "Unknown party"}</p>
                  <p className="muted">{(packet.party_type as string) ?? "n/a"}</p>
                </div>
                <div>
                  <h3>Risk</h3>
                  <p>{(packet.risk_rating as string) ?? "n/a"}</p>
                  <p className="muted">
                    Max score {packet.max_risk_score ?? "n/a"}
                  </p>
                </div>
                <div>
                  <h3>Window</h3>
                  <p>{dateFromMs(packet.case_window_start_ms_utc as number)}</p>
                  <p className="muted">{dateFromMs(packet.case_window_end_ms_utc as number)}</p>
                </div>
                <div>
                  <h3>Volume</h3>
                  <p>{currency(packet.amount_total_usd_case as number)}</p>
                  <p className="muted">{packet.txn_count_case ?? "n/a"} txns</p>
                </div>
              </div>

              <div className="packet-section">
                <h3>Alerts</h3>
                <div className="chips">
                  <span>High: {packet.alerts_high as number}</span>
                  <span>Medium: {packet.alerts_medium as number}</span>
                  <span>Low: {packet.alerts_low as number}</span>
                  <span>Total: {packet.alerts_count as number}</span>
                </div>
                {renderTable(
                  (packet.alerts as Array<Record<string, unknown>>) ?? [],
                  [
                    { key: "ts", label: "Timestamp" },
                    { key: "alert_id", label: "Alert ID" },
                    { key: "model_type", label: "Model" },
                    { key: "scenario_code", label: "Scenario" },
                    { key: "risk_score", label: "Risk Score" },
                    { key: "severity", label: "Severity" },
                    { key: "trigger_summary", label: "Summary" },
                    { key: "amount_total_usd", label: "Amount" },
                    { key: "txn_count", label: "Txn Count" },
                  ],
                  "No alerts captured."
                )}
              </div>

              <div className="packet-section">
                <h3>Top Counterparties</h3>
                {renderTable(
                  (packet.top_counterparties as Array<Record<string, unknown>>) ?? [],
                  [
                    { key: "counterparty_id", label: "Counterparty ID" },
                    { key: "counterparty_type", label: "Type" },
                    { key: "country", label: "Country" },
                    { key: "txn_count", label: "Txn Count" },
                    { key: "amount_total_usd", label: "Amount" },
                    { key: "intl_ratio", label: "Intl Ratio" },
                    { key: "last_txn_ms_utc", label: "Last Txn" },
                  ],
                  "No counterparties captured."
                )}
              </div>

              <div className="packet-section">
                <h3>Top Merchants</h3>
                {renderTable(
                  (packet.top_merchants as Array<Record<string, unknown>>) ?? [],
                  [
                    { key: "merchant_id", label: "Merchant ID" },
                    { key: "merchant_name", label: "Merchant" },
                    { key: "merchant_category", label: "Category" },
                    { key: "country", label: "Country" },
                    { key: "state", label: "State" },
                    { key: "txn_count", label: "Txn Count" },
                    { key: "amount_total_usd", label: "Amount" },
                  ],
                  "No merchants captured."
                )}
              </div>

              <div className="packet-section">
                <h3>Supporting Transactions</h3>
                {renderTable(
                  (packet.supporting_transactions as Array<Record<string, unknown>>) ?? [],
                  [
                    { key: "ts", label: "Timestamp" },
                    { key: "txn_id", label: "Txn ID" },
                    { key: "instrument_type", label: "Instrument" },
                    { key: "direction", label: "Direction" },
                    { key: "amount", label: "Amount" },
                    { key: "currency", label: "Currency" },
                    { key: "counterparty_id", label: "Counterparty" },
                    { key: "merchant_id", label: "Merchant" },
                    { key: "country", label: "Country" },
                    { key: "state", label: "State" },
                  ],
                  "No transactions captured."
                )}
              </div>

              <div className="packet-section">
                <h3>Timeline</h3>
                <div className="timeline">
                  {timeline.map((entry, index) => (
                    <div key={`${entry.case_id}-${index}`} className="timeline-row">
                      <span>{entry.txn_date_utc || "n/a"}</span>
                      <span>{entry.instrument_type || "n/a"}</span>
                      <span>{entry.txn_count} txns</span>
                      <span>{currency(entry.amount_total_usd)}</span>
                    </div>
                  ))}
                  {timeline.length === 0 && <p className="muted">No timeline entries.</p>}
                </div>
              </div>
            </div>
          )}
        </section>

        <section className="panel panel-right">
          <div className="panel-title">
            <h2>Copilot Panel</h2>
            <button onClick={runCopilot} disabled={!selectedCase || isRunningCopilot}>
              {isRunningCopilot ? "Running..." : "Generate Summary"}
            </button>
          </div>
          {!copilot && <p className="muted">Generate a summary to review.</p>}
          {copilot && (
            <div className="copilot">
              <div className="copilot-block">
                <h3>Case Summary</h3>
                <p>{copilot.case_summary}</p>
              </div>
              <div className="copilot-block">
                <h3>Key Indicators</h3>
                <ul>
                  {copilot.key_indicators.map((item, idx) => (
                    <li key={`ki-${idx}`}>{item.indicator}</li>
                  ))}
                </ul>
              </div>
              <div className="copilot-block">
                <h3>Benign Explanations</h3>
                <ul>
                  {copilot.benign_explanations_to_rule_out.map((item, idx) => (
                    <li key={`be-${idx}`}>{item.explanation}</li>
                  ))}
                </ul>
              </div>
              <div className="copilot-block">
                <h3>Policy Mapping</h3>
                <ul>
                  {copilot.policy_mapping.map((item, idx) => (
                    <li key={`pm-${idx}`}>{item.policy}</li>
                  ))}
                </ul>
              </div>
              <div className="copilot-block">
                <h3>Missing Information</h3>
                <ul>
                  {copilot.missing_information.map((item, idx) => (
                    <li key={`mi-${idx}`}>{item}</li>
                  ))}
                </ul>
              </div>
              <div className="copilot-block">
                <h3>Disposition</h3>
                <p>
                  {copilot.recommended_disposition} (confidence {copilot.confidence})
                </p>
              </div>
            </div>
          )}

          <div className="copilot-block">
            <h3>Decision & Narrative</h3>
            <label>
              Decision
              <input
                type="text"
                value={decision}
                onChange={(event) => setDecision(event.target.value)}
                placeholder="Enter decision"
              />
            </label>
            <label>
              Narrative
              <textarea
                value={narrative}
                onChange={(event) => setNarrative(event.target.value)}
                placeholder="Edit narrative draft"
                rows={6}
              />
            </label>
          </div>

          <div className="copilot-block">
            <h3>Feedback</h3>
            <label className="toggle">
              Helpful
              <input
                type="checkbox"
                checked={helpful}
                onChange={(event) => setHelpful(event.target.checked)}
              />
            </label>
            <label>
              What was wrong
              <textarea
                value={wrongParts}
                onChange={(event) => setWrongParts(event.target.value)}
                placeholder="One issue per line"
                rows={4}
              />
            </label>
            <label>
              Missing data
              <textarea
                value={missingData}
                onChange={(event) => setMissingData(event.target.value)}
                placeholder="One item per line"
                rows={3}
              />
            </label>
            <button className="primary" onClick={submitFeedback} disabled={!selectedCase}>
              Submit feedback
            </button>
          </div>
        </section>
      </main>
    </div>
  );
}
