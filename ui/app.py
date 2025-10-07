import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
from queue import Queue

ENTRY_W   = 40   # left side text entries
COMBO_W   = 36   # source/dest combo boxes
LOG_W     = 120  # scrolling text width
LOG_H     = 22   # Scrolling text height


class App:
    def __init__(self, controller):
        self.controller = controller

        # Tk root and vars
        self.root = tk.Tk()
        self.root.title("SP/OneDrive Copy Tool")

        self.stage_var = tk.StringVar(value="idle")

        self.tenant_var = tk.StringVar()
        self.client_var = tk.StringVar()
        self.secret_var = tk.StringVar()

        self.src_drive_var = tk.StringVar()
        self.src_parent_var = tk.StringVar()
        self.dest_drive_var = tk.StringVar()
        self.dest_parent_var = tk.StringVar()
        self.root_name_var = tk.StringVar(value="SRC_ROOT")

        self.src_site_name_var   = tk.StringVar()
        self.src_lib_name_var    = tk.StringVar()
        self.src_parent_name_var = tk.StringVar()

        self.dst_type_var          = tk.StringVar(value="sp")
        self.dest_site_name_var    = tk.StringVar()
        self.dest_lib_name_var     = tk.StringVar()
        self.dest_parent_name_var  = tk.StringVar()
        self.dest_user_query_var   = tk.StringVar()
        self.dest_user_name_var    = tk.StringVar()

        # Stats vars (ADD: eta_var)
        self.files_var    = tk.StringVar(value="0 / 0")
        self.rate_var     = tk.StringVar(value="0.00 MB/s")
        self.elapsed_var  = tk.StringVar(value="00:00:00")
        self.eta_var      = tk.StringVar(value="--:--:--")
        self.workers_var  = tk.StringVar(value="1")
        self.throttle_var = tk.StringVar(value="0")

        # logger
        self.LOGQ = Queue()

        # Build UI once
        self._build_ui()

        # Wire controller->app callbacks
        self.controller.set_callbacks(log=self.log, set_stage=self.set_stage)

        # Start log drain + stats ticker
        self._drain_log()
        self._tick_stats()

    # App <-> Controller callbacks
    def log(self, msg: str):
        self.LOGQ.put(msg.rstrip("\n"))

    def set_stage(self, text: str):
        self.stage_var.set(text)

    # UI construction
    def _build_ui(self):
        # Window size 
        self.root.geometry("1200x720")
        self.root.minsize(1000, 620)

        # root grid
        for c in range(3):
            self.root.columnconfigure(c, weight=1)
        self.root.rowconfigure(0, weight=0)   
        self.root.rowconfigure(1, weight=1)   
        self.root.rowconfigure(2, weight=0)   

        # Top strip frames
        top_fields = ttk.LabelFrame(self.root, text="Fields", padding=6)
        top_fields.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        top_fields.columnconfigure(1, weight=1) 

        top_source = ttk.LabelFrame(self.root, text="Source", padding=6)
        top_source.grid(row=0, column=1, sticky="nsew", padx=6, pady=6)
        top_source.columnconfigure(1, weight=1)

        top_stats = ttk.LabelFrame(self.root, text="Stats", padding=6)
        top_stats.grid(row=0, column=2, sticky="nsew", padx=6, pady=6)
        top_stats.columnconfigure(0, weight=1)
        top_stats.columnconfigure(1, weight=1)

        # Fields (IDs + creds)
        fields = [
            ("TENANT",     self.tenant_var),
            ("CLIENT",     self.client_var),
            ("SECRET",     self.secret_var),
            ("SRC_DRIVE",  self.src_drive_var),
            ("SRC_PARENT", self.src_parent_var),
            ("DEST_DRIVE", self.dest_drive_var),
            ("DEST_PARENT",self.dest_parent_var),
            ("ROOT_NAME",  self.root_name_var),
        ]
        for i, (label, var) in enumerate(fields):
            ttk.Label(top_fields, text=label).grid(row=i, column=0, sticky="w", padx=6, pady=4)
            ttk.Entry(
                top_fields,
                textvariable=var,
                width=ENTRY_W,
                show="*" if label == "SECRET" else ""
            ).grid(row=i, column=1, padx=6, pady=4, sticky="ew")

        # Stage readou(at end of Fields)
        stage_row = len(fields)
        ttk.Label(top_fields, text="Stage").grid(row=stage_row, column=0, sticky="w", padx=6, pady=(8, 0))
        ttk.Label(top_fields, textvariable=self.stage_var).grid(row=stage_row, column=1, sticky="w", padx=6, pady=(8, 0))

        #source pickers
        r = 0
        ttk.Label(top_source, text="Site").grid(row=r, column=0, sticky="w")
        self.src_site_combo = ttk.Combobox(top_source, textvariable=self.src_site_name_var, width=COMBO_W, state="readonly")
        self.src_site_combo.grid(row=r, column=1, sticky="ew", padx=6, pady=2); r += 1

        ttk.Label(top_source, text="Library").grid(row=r, column=0, sticky="w")
        self.src_lib_combo = ttk.Combobox(top_source, textvariable=self.src_lib_name_var, width=COMBO_W, state="readonly")
        self.src_lib_combo.grid(row=r, column=1, sticky="ew", padx=6, pady=2); r += 1

        ttk.Label(top_source, text="Parent folder").grid(row=r, column=0, sticky="w")
        self.src_parent_combo = ttk.Combobox(top_source, textvariable=self.src_parent_name_var, width=COMBO_W, state="readonly")
        self.src_parent_combo.grid(row=r, column=1, sticky="ew", padx=6, pady=2); r += 1

        # Destination mode (SP / OD)
        self._dest_section_row = r  
        mode = ttk.Frame(top_source); mode.grid(row=r, column=0, columnspan=2, sticky="w", pady=(4, 0)); r += 1
        ttk.Label(mode, text="Destination:").pack(side="left")
        ttk.Radiobutton(mode, text="SharePoint", value="sp", variable=self.dst_type_var,
                        command=self.toggle_dest_mode).pack(side="left", padx=(8, 0))
        ttk.Radiobutton(mode, text="OneDrive", value="od", variable=self.dst_type_var,
                        command=self.toggle_dest_mode).pack(side="left", padx=(8, 0))

        # Destination: SharePoint subframe
        self.dest_sp = ttk.Frame(top_source)
        ttk.Label(self.dest_sp, text="Site").grid(row=0, column=0, sticky="w")
        self.dest_site_combo = ttk.Combobox(self.dest_sp, textvariable=self.dest_site_name_var, width=COMBO_W, state="readonly")
        self.dest_site_combo.grid(row=0, column=1, sticky="ew", padx=6, pady=2)

        ttk.Label(self.dest_sp, text="Library").grid(row=1, column=0, sticky="w")
        self.dest_lib_combo = ttk.Combobox(self.dest_sp, textvariable=self.dest_lib_name_var, width=COMBO_W, state="readonly")
        self.dest_lib_combo.grid(row=1, column=1, sticky="ew", padx=6, pady=2)

        ttk.Label(self.dest_sp, text="Parent folder").grid(row=2, column=0, sticky="w")
        self.dest_parent_combo = ttk.Combobox(self.dest_sp, textvariable=self.dest_parent_name_var, width=COMBO_W, state="readonly")
        self.dest_parent_combo.grid(row=2, column=1, sticky="ew", padx=6, pady=2)

        # Destination: OneDrive subframe
        self.dest_od = ttk.Frame(top_source)
        ttk.Label(self.dest_od, text="User search").grid(row=0, column=0, sticky="w")
        self.dest_user_entry = ttk.Entry(self.dest_od, textvariable=self.dest_user_query_var, width=COMBO_W)
        self.dest_user_entry.grid(row=0, column=1, sticky="ew", padx=6, pady=2)
        ttk.Button(self.dest_od, text="Find users", command=self.on_search_users)\
            .grid(row=0, column=2, padx=6)
        self.dest_user_combo = ttk.Combobox(self.dest_od, textvariable=self.dest_user_name_var, width=COMBO_W, state="readonly")
        self.dest_user_combo.grid(row=1, column=1, sticky="ew", padx=6, pady=2)

        # Stats (labels inside top_stats)
        ttk.Label(top_stats, text="Files").grid(row=0, column=0, sticky="w")
        ttk.Label(top_stats, textvariable=self.files_var).grid(row=0, column=1, sticky="e")

        ttk.Label(top_stats, text="Rate").grid(row=1, column=0, sticky="w")
        ttk.Label(top_stats, textvariable=self.rate_var).grid(row=1, column=1, sticky="e")

        ttk.Label(top_stats, text="Elapsed").grid(row=2, column=0, sticky="w")
        ttk.Label(top_stats, textvariable=self.elapsed_var).grid(row=2, column=1, sticky="e")

        ttk.Label(top_stats, text="ETA").grid(row=3, column=0, sticky="w")
        ttk.Label(top_stats, textvariable=self.eta_var).grid(row=3, column=1, sticky="e")

        ttk.Label(top_stats, text="Workers").grid(row=4, column=0, sticky="w")
        ttk.Label(top_stats, textvariable=self.workers_var).grid(row=4, column=1, sticky="e")

        ttk.Label(top_stats, text="Throttles").grid(row=5, column=0, sticky="w")
        ttk.Label(top_stats, textvariable=self.throttle_var).grid(row=5, column=1, sticky="e")

        # Output (spans all 3 columns)
        out = ttk.LabelFrame(self.root, text="Output", padding=6)
        out.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=6, pady=6)
        out.columnconfigure(0, weight=1)
        out.rowconfigure(0, weight=1)

        self.log_box = ScrolledText(out, width=LOG_W, height=LOG_H, state="disabled",
                                    borderwidth=1, relief="solid")
        self.log_box.grid(row=0, column=0, sticky="nsew")

        #Buttons (bottom row)
        ttk.Button(self.root, text="Start",   command=self.on_start).grid( row=2, column=0, sticky="ew", padx=6, pady=6)
        ttk.Button(self.root, text="Cancel",  command=self.on_cancel).grid(row=2, column=1, sticky="ew", padx=6, pady=6)
        ttk.Button(self.root, text="Connect", command=self.on_connect).grid(row=2, column=2, sticky="ew", padx=6, pady=6)

        # Binders
        self.src_site_combo.bind("<<ComboboxSelected>>", self.on_src_site_selected)
        self.src_lib_combo.bind("<<ComboboxSelected>>", self.on_src_lib_selected)
        self.src_parent_combo.bind("<<ComboboxSelected>>", self.on_src_parent_chosen)
        # Destination bindings (SP)
        self.dest_site_combo.bind("<<ComboboxSelected>>", self.on_dest_site_selected)
        self.dest_lib_combo.bind("<<ComboboxSelected>>", self.on_dest_lib_selected)
        self.dest_parent_combo.bind("<<ComboboxSelected>>", self.on_dest_parent_chosen)
        # Destination bindings (OD)
        self.dest_user_combo.bind("<<ComboboxSelected>>", self.on_dest_user_chosen)

        #ode toggle wiring
        self.dst_type_var.trace_add("write", lambda *_: self.toggle_dest_mode())
        self.toggle_dest_mode()

    # Helpers for stats
    def _fmt_hms(self, seconds: float) -> str:
        s = max(0, int(seconds))
        h, s = divmod(s, 3600)
        m, s = divmod(s, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def _tick_stats(self):
        s = self.controller.get_stats()  

        files_total = s.get("files_total", 0)
        files_done  = s.get("files_done", 0)
        elapsed     = s.get("elapsed", 0.0)
        rate_bps    = s.get("rate", 0.0)
        workers     = s.get("workers", 1)
        throttles   = s.get("throttles_recent", 0)

        self.files_var.set(f"{files_done:,} / {files_total:,}")
        self.rate_var.set(f"{(rate_bps/1024/1024):.2f} MB/s")
        self.elapsed_var.set(self._fmt_hms(elapsed))
        self.workers_var.set(str(workers))
        self.throttle_var.set(str(throttles))

        # ETA (file-count based)
        if files_done > 0 and files_total > 0 and elapsed > 0:
            files_per_sec = files_done / elapsed
            remaining = max(0, files_total - files_done)
            eta_sec = remaining / files_per_sec if files_per_sec > 0 else 0
            self.eta_var.set(self._fmt_hms(eta_sec))
        else:
            self.eta_var.set("--:--:--")

        self.root.after(1000, self._tick_stats)

    # GUI handlers
    def on_connect(self):
        names = self.controller.connect(
            tenant=self.tenant_var.get(),
            client=self.client_var.get(),
            secret=self.secret_var.get(),
        )
        self.src_site_combo["values"] = names
        self.dest_site_combo["values"] = names
        self.log(f"[OK] Connected. Loaded {len(names)} sites. Pick site(s).")

    def on_src_site_selected(self, _=None):
        libs = self.controller.select_src_site(self.src_site_name_var.get())
        self.src_lib_combo["values"] = libs
        if libs:
            self.src_lib_combo.current(0)
            self.on_src_lib_selected()

    def on_src_lib_selected(self, _=None):
        res = self.controller.select_src_lib(self.src_lib_name_var.get())
        self.src_drive_var.set(res.get("drive_id") or "")
        self.src_parent_var.set(res.get("default_parent_id") or "root")
        names = res.get("parent_names", [])
        self.src_parent_combo["values"] = names
        if names:
            self.src_parent_combo.current(0)

    def on_src_parent_chosen(self, _=None):
        pass

    def on_dest_site_selected(self, _=None):
        libs = self.controller.select_dst_site(self.dest_site_name_var.get())
        self.dest_lib_combo["values"] = libs
        if libs:
            self.dest_lib_combo.current(0)
            self.on_dest_lib_selected()

    def on_dest_lib_selected(self, _=None):
        res = self.controller.select_dst_lib(self.dest_lib_name_var.get())
        self.dest_drive_var.set(res.get("drive_id") or "")
        self.dest_parent_var.set(res.get("default_parent_id") or "")
        names = res.get("parent_names", [])
        self.dest_parent_combo["values"] = names
        if names:
            self.dest_parent_combo.current(0)

    def on_dest_parent_chosen(self, _=None):
        pass

    def on_search_users(self):
        q = self.dest_user_query_var.get().strip()
        if not q:
            self.log("[INFO] Enter a user search (name or UPN).")
            return
        display = self.controller.search_users(q)
        self.dest_user_combo["values"] = display
        if display:
            self.dest_user_combo.current(0)
            self.on_dest_user_chosen()
        self.log(f"[DST-OD] Found {len(display)} users.")

    def on_dest_user_chosen(self, _=None):
        sel = self.dest_user_name_var.get()
        res = self.controller.choose_user(sel)
        if not res:
            self.log("[DST-OD] OneDrive not provisioned.")
            return
        self.dest_drive_var.set(res["drive_id"])
        self.dest_parent_var.set(res["root_id"])
        self.dest_parent_combo["values"] = res["parent_names"]
        if res["parent_names"]:
            self.dest_parent_combo.current(0)
        self.log(f"[DST-OD] Ready: drive {res['drive_id']}.")

    def on_start(self):
        cfg = {
            "SRC_DRIVE":   self.src_drive_var.get().strip(),
            "SRC_PARENT":  self.src_parent_var.get().strip(),
            "DEST_DRIVE":  self.dest_drive_var.get().strip(),
            "DEST_PARENT": self.dest_parent_var.get().strip(),
            "ROOT_NAME":   self.root_name_var.get().strip(),
            "TENANT":      self.tenant_var.get().strip(),
            "CLIENT":      self.client_var.get().strip(),
            "SECRET":      self.secret_var.get().strip(),
        }
        self.controller.start_job(cfg)

    def on_cancel(self):
        self.controller.cancel_job()

    def toggle_dest_mode(self):
    # Sub-frame should sit directly UNDER the radio row.
        base_row = self._dest_section_row + 1

        # Always hide both first (
        try: self.dest_sp.grid_remove()
        except Exception: pass
        try: self.dest_od.grid_remove()
        except Exception: pass

        if self.dst_type_var.get() == "od":
            # Show OneDrive panel
            self.dest_od.grid(row=base_row, column=0, columnspan=2,
                            sticky="ew", padx=6, pady=(2, 0))
        else:
            # Show SharePoint panel
            self.dest_sp.grid(row=base_row, column=0, columnspan=2,
                            sticky="ew", padx=6, pady=(2, 0))

    # log drain
    def _drain_log(self):
        try:
            while True:
                line = self.LOGQ.get_nowait()
                self.log_box.configure(state="normal")
                self.log_box.insert("end", line + "\n")
                self.log_box.see("end")
                self.log_box.configure(state="disabled")
        except Exception:
            pass
        self.root.after(150, self._drain_log)

    def run(self):
        
        self.root.mainloop()
