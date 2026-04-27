const state = {
  busy: false,
  folders: {
    music: "",
    video: "",
  },
  draftDirty: false,
  activeAction: "",
};

function qs(selector) {
  return document.querySelector(selector);
}

function statusChip(value) {
  const raw = String(value || "-");
  const lower = raw.toLowerCase();
  let tone = "status-ready";
  if (lower.includes("error")) tone = "status-error";
  else if (lower.includes("pending") || lower.includes("retry")) tone = "status-warning";
  else if (lower.includes("videocreated") || lower.includes("processed") || lower.includes("rendered")) tone = "status-progress";
  else if (lower.includes("premiumdone")) tone = "status-done";
  return `<span class="status-chip ${tone}">${raw}</span>`;
}

function fileNameOnly(path) {
  if (!path) return "";
  return String(path).split(/[\\/]/).pop();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const raw = await response.text();
  let data;
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch {
    throw new Error(raw || `Request failed: ${response.status}`);
  }
  if (!response.ok) {
    throw new Error(data.error || `Request failed: ${response.status}`);
  }
  return data;
}

function setBusy(busy) {
  state.busy = busy;
  document.querySelectorAll("button, select, input").forEach((node) => {
    if (node.dataset.action) {
      return;
    }
    node.disabled = busy;
  });
  renderWorkflowButtons();
}

function updateStatusBanner(title, text, tone = "idle") {
  const banner = qs("#statusBanner");
  const titleNode = qs("#statusTitle");
  const textNode = qs("#messageLine");
  banner.className = `status-banner status-${tone}`;
  titleNode.textContent = title || "Ready";
  textNode.textContent = text || "Ready.";
}

function actionRunningLabel(action) {
  if (action === "render") return "Rendering...";
  if (action === "process") return "Processing...";
  if (action === "upload") return "Uploading...";
  if (action === "premium") return "Adding Premium...";
  if (action === "next") return "Running...";
  return "Running...";
}

function renderWorkflowButtons() {
  document.querySelectorAll("[data-action]").forEach((button) => {
    const action = button.dataset.action;
    const baseLabel = button.dataset.label || button.textContent;
    button.dataset.label = baseLabel;
    if (state.busy) {
      if (state.activeAction && action !== state.activeAction) {
        button.classList.remove("is-hidden");
        button.classList.add("is-dimmed");
        button.disabled = true;
        button.textContent = baseLabel;
      } else {
        button.classList.remove("is-hidden");
        button.classList.remove("is-dimmed");
        button.disabled = true;
        button.textContent = actionRunningLabel(action);
      }
      return;
    }
    button.classList.remove("is-hidden");
    button.classList.remove("is-dimmed");
    button.disabled = false;
    button.textContent = baseLabel;
  });
}

function renderPipelineRows(rows) {
  const tbody = qs("#pipelineBody");
  tbody.innerHTML = "";
  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.index + 1}</td>
      <td><strong>${escapeHtml(row.output_base)}</strong></td>
      <td>${escapeHtml(fileNameOnly(row.music_path))}</td>
      <td>${escapeHtml(fileNameOnly(row.video_path))}</td>
      <td>${statusChip(row.render_status || "-")}</td>
      <td>${statusChip(row.process_status || "-")}</td>
      <td>${statusChip(row.upload_status || "-")}</td>
      <td class="mono">${escapeHtml(row.video_id || "-")}</td>
      <td>${statusChip(row.premium_status || "-")}</td>
      <td class="mono">${escapeHtml(row.addsub_done || "0/12")}</td>
      <td>${statusChip(row.overall_status || "-")}</td>
      <td class="note-cell">${escapeHtml(row.last_error || (row.overall_status === "PremiumDone" ? "Completed successfully." : "-"))}</td>
      <td><div class="pipeline-actions"><button class="subtle ghost" data-output="${escapeHtml(row.output_base)}">Retry</button></div></td>
    `;
    tr.querySelector("button")?.addEventListener("click", () => retryRow(row.output_base));
    tbody.appendChild(tr);
  }
}

function renderChannelOptions(channels, selected) {
  const select = qs("#channelSelect");
  const existing = select.value;
  select.innerHTML = "";
  for (const channel of channels) {
    const option = document.createElement("option");
    option.value = channel;
    option.textContent = channel;
    select.appendChild(option);
  }
  select.value = selected || existing || channels[0] || "";
}

async function refreshState() {
  const data = await fetchJSON("/api/state");
  if (!state.draftDirty) {
    state.folders.music = data.music_folder || "";
    state.folders.video = data.video_folder || "";
    qs("#musicFolderInput").value = state.folders.music;
    qs("#videoFolderInput").value = state.folders.video;
  }
  renderPipelineRows(data.rows || []);
  renderChannelOptions(data.channels || [], data.selected_channel || "");
  const wasBusy = state.busy;
  state.busy = Boolean(data.busy);
  if (wasBusy && state.busy && !state.activeAction) {
    state.activeAction = "next";
  }
  if (!state.busy) {
    state.activeAction = "";
  }
  setBusy(Boolean(data.busy));
  if (data.busy) updateStatusBanner("Running", data.activity || "A workflow step is running...", "running");
  else if (data.last_error) updateStatusBanner("Error", data.last_error, "error");
  else if (data.last_message) updateStatusBanner("Success", data.last_message, "success");
  else updateStatusBanner("Ready", data.activity || "Choose folders and run a phase.", "idle");
}

async function saveFolders({ refresh = true } = {}) {
  state.folders.music = qs("#musicFolderInput").value.trim();
  state.folders.video = qs("#videoFolderInput").value.trim();
  const payload = JSON.stringify({
    music_folder: state.folders.music,
    video_folder: state.folders.video,
  });
  try {
    await fetchJSON("/api/folders", {
      method: "POST",
      body: payload,
    });
  } catch (error) {
    if (!String(error.message || "").includes("Not found")) {
      throw error;
    }
    await fetchJSON("/api/rows", {
      method: "POST",
      body: payload,
    });
  }
  state.draftDirty = false;
  if (refresh) {
    await refreshState();
  }
}

async function clearFolders() {
  state.folders.music = "";
  state.folders.video = "";
  state.draftDirty = false;
  qs("#musicFolderInput").value = "";
  qs("#videoFolderInput").value = "";
  await fetchJSON("/api/clear", { method: "POST", body: "{}" });
  await refreshState();
}

async function retryRow(outputBase) {
  await fetchJSON("/api/retry", {
    method: "POST",
    body: JSON.stringify({ output_base: outputBase }),
  });
  await refreshState();
}

async function runAction(action) {
  state.activeAction = action;
  state.busy = true;
  updateStatusBanner("Running", `${actionRunningLabel(action)} Please wait...`, "running");
  setBusy(true);
  renderWorkflowButtons();
  await saveFolders({ refresh: false });
  const data = await fetchJSON("/api/action", {
    method: "POST",
    body: JSON.stringify({ action }),
  });
  state.busy = Boolean(data.busy);
  if (!state.busy) {
    state.activeAction = "";
  }
  setBusy(state.busy);
  if (data.busy) {
    updateStatusBanner("Running", data.activity || `${actionRunningLabel(action)} Please wait...`, "running");
  }
  await refreshState();
}

async function changeChannel() {
  const channel = qs("#channelSelect").value;
  await fetchJSON("/api/channel", {
    method: "POST",
    body: JSON.stringify({ channel }),
  });
  await refreshState();
}

function bindStaticActions() {
  qs("#musicFolderInput").addEventListener("input", () => {
    state.draftDirty = true;
  });
  qs("#videoFolderInput").addEventListener("input", () => {
    state.draftDirty = true;
  });
  qs("#saveFoldersBtn").addEventListener("click", () => saveFolders().catch(showError));
  qs("#clearFoldersBtn").addEventListener("click", () => clearFolders().catch(showError));
  qs("#channelSelect").addEventListener("change", () => changeChannel().catch(showError));
  document.querySelectorAll("[data-action]").forEach((button) => {
    button.addEventListener("click", () => runAction(button.dataset.action).catch(showError));
  });
}

function showError(error) {
  updateStatusBanner("Error", error.message || String(error), "error");
}

async function bootstrap() {
  bindStaticActions();
  await refreshState();
  setInterval(() => refreshState().catch(showError), 2000);
}

bootstrap().catch(showError);
