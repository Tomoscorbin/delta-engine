window.mermaid = window.mermaid || {};
if (typeof mermaid.initialize === "function") {
  mermaid.initialize({ startOnLoad: true, theme: "neutral" });
} else {
  document.addEventListener("DOMContentLoaded", function () {
    if (typeof mermaid !== "undefined" && typeof mermaid.initialize === "function") {
      mermaid.initialize({ startOnLoad: true, theme: "neutral" });
    }
  });
}
