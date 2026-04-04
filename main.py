"""SQL Server Stress Test Tool — GUI application."""

import json
import os
import queue
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
import logging

import matplotlib
matplotlib.use("TkAgg")
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from auth import EntraAuthProvider
from engine import StressEngine


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("SQL Server Stress Test Tool")
        root.geometry("1350x960")
        root.minsize(1050, 750)

        self.servers: list[tuple[str, str]] = []
        self.engine: StressEngine | None = None
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.running = False
        self.chart_data: dict[str, list] = self._empty_chart_data()

        self._init_logging()
        self._init_styles()
        self._build_ui()

        # kick off periodic UI refreshes
        self._tick_metrics()
        self._tick_log()
        self._tick_chart()

    # ── helpers ───────────────────────────────────────────────

    @staticmethod
    def _empty_chart_data():
        return {"time": [], "success": [], "errors": [], "conns": [], "workers": []}

    def _init_logging(self):
        os.makedirs("logs", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.logger = logging.getLogger("stress")
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(
            os.path.join("logs", f"stress_{ts}.log"), encoding="utf-8"
        )
        fh.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
        self.logger.addHandler(fh)

    def _init_styles(self):
        s = ttk.Style()
        s.configure("Hero.TLabel",    font=("Segoe UI", 30, "bold"), foreground="#0078D4")
        s.configure("Big.TLabel",     font=("Segoe UI", 24, "bold"))
        s.configure("Green.TLabel",   font=("Segoe UI", 24, "bold"), foreground="#107C10")
        s.configure("Red.TLabel",     font=("Segoe UI", 24, "bold"), foreground="#D13438")
        s.configure("Blue.TLabel",    font=("Segoe UI", 24, "bold"), foreground="#0078D4")
        s.configure("GreenZ.TLabel",  font=("Segoe UI", 24, "bold"), foreground="#107C10")
        s.configure("Sub.TLabel",     font=("Segoe UI", 9))
        s.configure("Status.TLabel",  font=("Segoe UI", 10, "italic"))

    # ── UI construction ──────────────────────────────────────

    def _build_ui(self):
        main = ttk.Frame(self.root, padding=8)
        main.pack(fill=tk.BOTH, expand=True)

        self._build_config_row(main)
        self._build_test_row(main)
        self._build_metrics_row(main)
        self._build_bottom(main)

    # ---- config row: servers + auth ----

    def _build_config_row(self, parent):
        row = ttk.Frame(parent)
        row.pack(fill=tk.X, pady=(0, 4))

        # ── Server list ──
        sf = ttk.LabelFrame(row, text=" SQL Servers ", padding=6)
        sf.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 4))

        entry_r = ttk.Frame(sf)
        entry_r.pack(fill=tk.X)
        ttk.Label(entry_r, text="Server:").pack(side=tk.LEFT)
        self.server_var = tk.StringVar()
        ttk.Entry(entry_r, textvariable=self.server_var, width=30).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Label(entry_r, text="Database:").pack(side=tk.LEFT, padx=(10, 0))
        self.db_var = tk.StringVar()
        ttk.Entry(entry_r, textvariable=self.db_var, width=20).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(entry_r, text="Add Server", command=self._add_server).pack(
            side=tk.LEFT, padx=6
        )

        list_r = ttk.Frame(sf)
        list_r.pack(fill=tk.BOTH, expand=True, pady=4)
        self.server_listbox = tk.Listbox(list_r, height=3, font=("Consolas", 9))
        self.server_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb = ttk.Scrollbar(list_r, orient=tk.VERTICAL, command=self.server_listbox.yview)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.server_listbox.config(yscrollcommand=sb.set)

        ttk.Button(sf, text="Remove Selected", command=self._remove_server).pack(
            anchor=tk.W
        )

        # ── Auth ──
        af = ttk.LabelFrame(row, text=" Entra ID — Service Principal ", padding=6)
        af.pack(side=tk.LEFT, fill=tk.Y, padx=(4, 0))

        self.tenant_var = tk.StringVar()
        self.client_id_var = tk.StringVar()
        self.client_secret_var = tk.StringVar()

        for i, (label, var, show) in enumerate(
            [
                ("Tenant ID:", self.tenant_var, ""),
                ("Client ID:", self.client_id_var, ""),
                ("Client Secret:", self.client_secret_var, "*"),
            ]
        ):
            ttk.Label(af, text=label).grid(row=i, column=0, sticky=tk.W, pady=2)
            ttk.Entry(af, textvariable=var, width=38, show=show).grid(
                row=i, column=1, padx=4, pady=2
            )

        ttk.Button(af, text="Test Auth", command=self._test_auth).grid(
            row=3, column=1, sticky=tk.E, pady=(6, 0)
        )

    # ---- test row: query + controls ----

    def _build_test_row(self, parent):
        row = ttk.Frame(parent)
        row.pack(fill=tk.X, pady=4)

        # query
        qf = ttk.LabelFrame(row, text=" SQL Query ", padding=6)
        qf.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 4))
        self.query_text = tk.Text(qf, height=3, font=("Consolas", 10))
        self.query_text.pack(fill=tk.BOTH, expand=True)
        self.query_text.insert("1.0", "SELECT 1")

        # controls
        cf = ttk.LabelFrame(row, text=" Controls ", padding=6)
        cf.pack(side=tk.LEFT, fill=tk.Y, padx=(4, 0))

        r1 = ttk.Frame(cf)
        r1.pack(fill=tk.X, pady=2)
        ttk.Label(r1, text="Workers:").pack(side=tk.LEFT)
        self.workers_var = tk.IntVar(value=10)
        ttk.Spinbox(r1, from_=1, to=2000, textvariable=self.workers_var, width=7).pack(
            side=tk.LEFT, padx=4
        )

        r2 = ttk.Frame(cf)
        r2.pack(fill=tk.X, pady=2)
        ttk.Label(r2, text="Delay (ms):").pack(side=tk.LEFT)
        self.delay_var = tk.IntVar(value=0)
        ttk.Spinbox(
            r2, from_=0, to=60000, textvariable=self.delay_var, width=7
        ).pack(side=tk.LEFT, padx=4)

        btn_r = ttk.Frame(cf)
        btn_r.pack(fill=tk.X, pady=(6, 2))
        self.start_btn = ttk.Button(
            btn_r, text="\u25B6  Start Test", command=self._start_test
        )
        self.start_btn.pack(side=tk.LEFT, padx=2)
        self.stop_btn = ttk.Button(
            btn_r, text="\u25A0  Stop", command=self._stop_test, state=tk.DISABLED
        )
        self.stop_btn.pack(side=tk.LEFT, padx=2)

        add_r = ttk.Frame(cf)
        add_r.pack(fill=tk.X, pady=2)
        self.add_count_var = tk.IntVar(value=10)
        ttk.Spinbox(
            add_r, from_=1, to=500, textvariable=self.add_count_var, width=5
        ).pack(side=tk.LEFT, padx=2)
        self.add_btn = ttk.Button(
            add_r, text="+ Workers", command=self._add_workers, state=tk.DISABLED
        )
        self.add_btn.pack(side=tk.LEFT, padx=2)

        settings_r = ttk.Frame(cf)
        settings_r.pack(fill=tk.X, pady=(6, 2))
        ttk.Button(settings_r, text="Save Settings", command=self._save_settings).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(settings_r, text="Load Settings", command=self._load_settings).pack(
            side=tk.LEFT, padx=2
        )

    # ---- metrics row ----

    def _build_metrics_row(self, parent):
        mf = ttk.LabelFrame(parent, text=" Live Metrics ", padding=10)
        mf.pack(fill=tk.X, pady=4)

        definitions = [
            ("Connections / Min", "cpm", "Hero.TLabel"),
            ("Active Workers",    "aw",  "Blue.TLabel"),
            ("Queries / Min",     "qpm", "Green.TLabel"),
            ("Errors / Min",      "epm", "Red.TLabel"),
            ("Success Rate",      "sr",  "Green.TLabel"),
            ("Elapsed",           "el",  "Big.TLabel"),
        ]

        self.metric_labels: dict[str, ttk.Label] = {}
        for i, (title, key, style) in enumerate(definitions):
            card = ttk.Frame(mf, padding=4)
            card.grid(row=0, column=i, padx=12, sticky=tk.N)
            ttk.Label(card, text=title, style="Sub.TLabel").pack()
            lbl = ttk.Label(card, text="\u2014", style=style)
            lbl.pack()
            self.metric_labels[key] = lbl

        mf.columnconfigure(list(range(len(definitions))), weight=1)

        self.totals_label = ttk.Label(mf, text="", style="Sub.TLabel")
        self.totals_label.grid(
            row=1, column=0, columnspan=len(definitions), sticky=tk.W, pady=(6, 0)
        )

        self.status_label = ttk.Label(mf, text="Idle", style="Status.TLabel")
        self.status_label.grid(
            row=1,
            column=len(definitions) - 1,
            sticky=tk.E,
            pady=(6, 0),
        )

    # ---- bottom: chart + log ----

    def _build_bottom(self, parent):
        pw = ttk.PanedWindow(parent, orient=tk.VERTICAL)
        pw.pack(fill=tk.BOTH, expand=True, pady=4)

        # chart
        chart_frame = ttk.LabelFrame(pw, text=" Performance Chart ")
        self.fig = Figure(figsize=(10, 2.5), dpi=96, facecolor="#FAFAFA")
        self.ax1 = self.fig.add_subplot(111)
        self.ax2 = self.ax1.twinx()
        self.fig.subplots_adjust(left=0.06, right=0.94, top=0.90, bottom=0.18)
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        pw.add(chart_frame, weight=3)

        # log
        log_frame = ttk.LabelFrame(pw, text=" Log Output ")
        self.log_text = tk.Text(
            log_frame, height=7, font=("Consolas", 8), wrap=tk.NONE
        )
        sy = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        sx = ttk.Scrollbar(log_frame, orient=tk.HORIZONTAL, command=self.log_text.xview)
        self.log_text.config(yscrollcommand=sy.set, xscrollcommand=sx.set)
        sy.pack(side=tk.RIGHT, fill=tk.Y)
        sx.pack(side=tk.BOTTOM, fill=tk.X)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # tag for colouring error lines
        self.log_text.tag_configure("error", foreground="#D13438")
        pw.add(log_frame, weight=2)

    # ── settings save / load ──────────────────────────────────

    def _gather_settings(self) -> dict:
        return {
            "servers": self.servers,
            "tenant_id": self.tenant_var.get(),
            "client_id": self.client_id_var.get(),
            "client_secret": self.client_secret_var.get(),
            "query": self.query_text.get("1.0", tk.END).strip(),
            "workers": self.workers_var.get(),
            "delay_ms": self.delay_var.get(),
        }

    def _apply_settings(self, data: dict):
        # servers
        self.servers.clear()
        self.server_listbox.delete(0, tk.END)
        for s, d in data.get("servers", []):
            self.servers.append((s, d))
            self.server_listbox.insert(tk.END, f"{s}  /  {d}")

        # auth
        self.tenant_var.set(data.get("tenant_id", ""))
        self.client_id_var.set(data.get("client_id", ""))
        self.client_secret_var.set(data.get("client_secret", ""))

        # query
        self.query_text.delete("1.0", tk.END)
        self.query_text.insert("1.0", data.get("query", "SELECT 1"))

        # controls
        self.workers_var.set(data.get("workers", 10))
        self.delay_var.set(data.get("delay_ms", 0))

    def _save_settings(self):
        path = filedialog.asksaveasfilename(
            title="Save Settings",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self._gather_settings(), f, indent=2)
            messagebox.showinfo("Saved", f"Settings saved to {path}")
        except Exception as exc:
            messagebox.showerror("Save Error", str(exc))

    def _load_settings(self):
        path = filedialog.askopenfilename(
            title="Load Settings",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._apply_settings(data)
            messagebox.showinfo("Loaded", f"Settings loaded from {path}")
        except Exception as exc:
            messagebox.showerror("Load Error", str(exc))

    # ── server management ────────────────────────────────────

    def _add_server(self):
        s = self.server_var.get().strip()
        d = self.db_var.get().strip()
        if not s or not d:
            messagebox.showwarning("Input Required", "Enter both Server and Database.")
            return
        self.servers.append((s, d))
        self.server_listbox.insert(tk.END, f"{s}  /  {d}")
        self.server_var.set("")
        self.db_var.set("")

    def _remove_server(self):
        sel = self.server_listbox.curselection()
        if sel:
            idx = sel[0]
            self.server_listbox.delete(idx)
            del self.servers[idx]

    # ── auth test ────────────────────────────────────────────

    def _test_auth(self):
        tenant = self.tenant_var.get().strip()
        client_id = self.client_id_var.get().strip()
        secret = self.client_secret_var.get().strip()
        if not all([tenant, client_id, secret]):
            messagebox.showwarning("Missing", "Fill in all authentication fields.")
            return

        def run():
            try:
                EntraAuthProvider(tenant, client_id, secret).get_token()
                self.root.after(
                    0, lambda: messagebox.showinfo("Success", "Token acquired successfully.")
                )
            except Exception as exc:
                self.root.after(
                    0, lambda: messagebox.showerror("Auth Failed", str(exc))
                )

        threading.Thread(target=run, daemon=True).start()

    # ── test lifecycle ───────────────────────────────────────

    def _start_test(self):
        # validation
        if not self.servers:
            messagebox.showwarning("Servers", "Add at least one SQL Server.")
            return
        tenant = self.tenant_var.get().strip()
        client_id = self.client_id_var.get().strip()
        secret = self.client_secret_var.get().strip()
        if not all([tenant, client_id, secret]):
            messagebox.showwarning("Auth", "Fill in all authentication fields.")
            return
        sql = self.query_text.get("1.0", tk.END).strip()
        if not sql:
            messagebox.showwarning("Query", "Enter a SQL query to execute.")
            return

        workers = self.workers_var.get()
        if not messagebox.askyesno(
            "Confirm",
            f"Launch {workers} concurrent worker(s) against "
            f"{len(self.servers)} server(s)?\n\n"
            "This will generate significant load on the target server(s).",
        ):
            return

        try:
            auth = EntraAuthProvider(tenant, client_id, secret)
        except Exception as exc:
            messagebox.showerror("Auth Error", str(exc))
            return

        def on_log(msg):
            self.logger.info(msg)
            self.log_queue.put(msg)

        self.chart_data = self._empty_chart_data()
        delay = self.delay_var.get() / 1000.0

        self.engine = StressEngine(
            servers=self.servers.copy(),
            auth_provider=auth,
            query=sql,
            on_log=on_log,
            delay=delay,
        )
        self.engine.start(workers)
        self.running = True

        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.add_btn.config(state=tk.NORMAL)
        self.status_label.config(text="Running")

    def _stop_test(self):
        if not self.engine:
            return
        self.stop_btn.config(state=tk.DISABLED)
        self.status_label.config(text="Stopping …")

        def do_stop():
            self.engine.stop()
            self.running = False
            self.root.after(0, lambda: self.start_btn.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.add_btn.config(state=tk.DISABLED))
            self.root.after(0, lambda: self.status_label.config(text="Stopped"))

        threading.Thread(target=do_stop, daemon=True).start()

    def _add_workers(self):
        if self.engine and self.running:
            self.engine.add_workers(self.add_count_var.get())

    # ── periodic UI updates ──────────────────────────────────

    def _tick_metrics(self):
        if self.engine and self.running:
            sn = self.engine.metrics.snapshot()

            self.metric_labels["cpm"].config(text=f'{sn["connections_per_min"]:,}')
            self.metric_labels["aw"].config(text=str(sn["active_workers"]))
            self.metric_labels["qpm"].config(text=f'{sn["success_per_min"]:,}')
            self.metric_labels["epm"].config(text=f'{sn["errors_per_min"]:,}')
            self.metric_labels["sr"].config(text=f'{sn["success_rate"]:.1f}%')

            m, s = divmod(int(sn["elapsed"]), 60)
            h, m = divmod(m, 60)
            self.metric_labels["el"].config(text=f"{h:02d}:{m:02d}:{s:02d}")

            self.totals_label.config(
                text=(
                    f"Total:  {sn['total_success']:,} successful   ·   "
                    f"{sn['total_errors']:,} errors"
                )
            )

            # color-code error metric
            if sn["errors_per_min"] > 0:
                self.metric_labels["epm"].config(style="Red.TLabel")
            else:
                self.metric_labels["epm"].config(style="GreenZ.TLabel")

        self.root.after(1000, self._tick_metrics)

    def _tick_log(self):
        count = 0
        while count < 150:
            try:
                msg = self.log_queue.get_nowait()
            except queue.Empty:
                break
            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            line = f"[{ts}] {msg}\n"
            tag = "error" if "| ERROR |" in msg else ""
            self.log_text.insert(tk.END, line, tag)
            count += 1

        if count:
            self.log_text.see(tk.END)
            total_lines = int(self.log_text.index("end-1c").split(".")[0])
            if total_lines > 2000:
                self.log_text.delete("1.0", f"{total_lines - 1500}.0")

        self.root.after(250, self._tick_log)

    def _tick_chart(self):
        if self.engine and self.running:
            sn = self.engine.metrics.snapshot()
            cd = self.chart_data
            cd["time"].append(sn["elapsed"])
            cd["success"].append(sn["success_per_min"])
            cd["errors"].append(sn["errors_per_min"])
            cd["conns"].append(sn["connections_per_min"])
            cd["workers"].append(sn["active_workers"])

            t = cd["time"]
            self.ax1.clear()
            self.ax2.clear()

            # primary axis — throughput
            self.ax1.plot(
                t, cd["conns"], color="#0078D4", linewidth=2.2, label="Connections/Min"
            )
            self.ax1.plot(
                t, cd["success"], color="#107C10", linewidth=1.4, label="Success/Min"
            )
            self.ax1.fill_between(t, cd["errors"], color="#D13438", alpha=0.25)
            self.ax1.plot(
                t, cd["errors"], color="#D13438", linewidth=1.4, label="Errors/Min"
            )
            self.ax1.set_xlabel("Elapsed (s)", fontsize=8)
            self.ax1.set_ylabel("Per Minute", fontsize=8)
            self.ax1.legend(loc="upper left", fontsize=7)
            self.ax1.grid(True, alpha=0.15)
            self.ax1.tick_params(labelsize=7)

            # secondary axis — workers
            self.ax2.plot(
                t,
                cd["workers"],
                color="#FFB900",
                linewidth=1.2,
                linestyle="--",
                label="Active Workers",
            )
            self.ax2.set_ylabel("Workers", fontsize=8)
            self.ax2.legend(loc="upper right", fontsize=7)
            self.ax2.tick_params(labelsize=7)

            self.canvas.draw_idle()

        self.root.after(2000, self._tick_chart)


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
