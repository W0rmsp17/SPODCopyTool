

import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
from queue import Queue

ENTRY_W   = 40   # left side text enties
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

        # logger
        self.LOGQ = Queue()

        # build gUI
        self._build_ui()

        # wire callbacks for controller to app
        self.controller.set_callbacks(log=self.log, set_stage=self.set_stage)

        # start draining log
        self._drain_log()

    # App <-> Controller callbacks
    def log(self, msg: str):
        self.LOGQ.put(msg.rstrip("\n"))

    def set_stage(self, text: str):
        self.stage_var.set(text)

    # UI construction
    def _build_ui(self):
        # Sizing constants
        ENTRY_W = 40   # left-side text entries
        COMBO_W = 36   # source/dest comboboxes (same width both sides)
        LOG_W   = 120  # ScrolledText width (chars)
        LOG_H   = 22   # ScrolledText height (rows)

        # Panels
        left  = ttk.Frame(self.root)
        right = ttk.Frame(self.root)
        left.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        right.grid(row=0, column=1, sticky="nsew", padx=8, pady=8)

        # Window layout: left grows, right fixed
        self.root.columnconfigure(0, weight=1)
        self.root.columnconfigure(1, weight=0)
        self.root.rowconfigure(0, weight=1)  # allow vertical growth for left output

        # Left: only column 1 grows (for output)
        left.columnconfigure(0, weight=0)
        left.columnconfigure(1, weight=1)

        # Right: keep controls fixed width
        right.columnconfigure(0, weight=0)
        right.columnconfigure(1, weight=0)

        # Optional: starting size + minimums
        self.root.geometry("1200x720")
        self.root.minsize(1000, 600)

        # Base fields (IDs + creds)
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
            ttk.Label(left, text=label).grid(row=i, column=0, sticky="w", padx=6, pady=4)
            ttk.Entry(
                left,
                textvariable=var,
                width=ENTRY_W,
                show="*" if label == "SECRET" else ""
            ).grid(row=i, column=1, padx=6, pady=4, sticky="w")  # fixed width, no stretch

        # Output: bordered, larger, expands
        out = ttk.LabelFrame(left, text="Output", padding=6)
        out.grid(row=len(fields), column=0, columnspan=2, padx=6, pady=(8, 2), sticky="nsew")

        self.log_box = ScrolledText(out, width=LOG_W, height=LOG_H, state="disabled",
                                    borderwidth=1, relief="solid")
        self.log_box.grid(row=0, column=0, sticky="nsew")
        out.columnconfigure(0, weight=1)
        out.rowconfigure(0, weight=1)
        left.rowconfigure(len(fields), weight=1)  # let the output row expand

        # Stage + controls
        row_controls = len(fields) + 1
        ttk.Label(left, text="Stage").grid(row=row_controls, column=0, sticky="w", padx=6)
        ttk.Label(left, textvariable=self.stage_var).grid(row=row_controls, column=1, sticky="w", padx=6)

        row_controls += 1
        ttk.Button(left, text="Start",  command=self.on_start).grid(row=row_controls, column=0, pady=10, sticky="w")
        ttk.Button(left, text="Cancel", command=self.on_cancel).grid(row=row_controls, column=1, pady=10, sticky="e")

        # Right: Connect button
        ttk.Button(right, text="Connect", command=self.on_connect)\
            .grid(row=0, column=1, sticky="e", padx=6, pady=(0, 6))

        # Right: Source pickers (fixed width)
        r = 1
        ttk.Label(right, text="Source").grid(row=r, column=0, sticky="w"); r += 1

        ttk.Label(right, text="Site").grid(row=r, column=0, sticky="w")
        self.src_site_combo = ttk.Combobox(right, textvariable=self.src_site_name_var, width=COMBO_W, state="readonly")
        self.src_site_combo.grid(row=r, column=1, sticky="w", padx=6, pady=2); r += 1

        ttk.Label(right, text="Library").grid(row=r, column=0, sticky="w")
        self.src_lib_combo = ttk.Combobox(right, textvariable=self.src_lib_name_var, width=COMBO_W, state="readonly")
        self.src_lib_combo.grid(row=r, column=1, sticky="w", padx=6, pady=2); r += 1

        ttk.Label(right, text="Parent folder").grid(row=r, column=0, sticky="w")
        self.src_parent_combo = ttk.Combobox(right, textvariable=self.src_parent_name_var, width=COMBO_W, state="readonly")
        self.src_parent_combo.grid(row=r, column=1, sticky="w", padx=6, pady=2); r += 2

        # Destination mode
        mode = ttk.Frame(right); mode.grid(row=r, column=0, columnspan=2, sticky="w"); r += 1
        ttk.Label(mode, text="Destination:").pack(side="left")
        ttk.Radiobutton(mode, text="SharePoint", value="sp", variable=self.dst_type_var,
                        command=self.toggle_dest_mode).pack(side="left", padx=(8, 0))
        ttk.Radiobutton(mode, text="OneDrive", value="od", variable=self.dst_type_var,
                        command=self.toggle_dest_mode).pack(side="left", padx=(8, 0))

        # Destination: SharePoint frame (fixed widths)
        self.dest_sp = ttk.Frame(right)
        ttk.Label(self.dest_sp, text="Site").grid(row=0, column=0, sticky="w")
        self.dest_site_combo = ttk.Combobox(self.dest_sp, textvariable=self.dest_site_name_var, width=COMBO_W, state="readonly")
        self.dest_site_combo.grid(row=0, column=1, sticky="w", padx=6, pady=2)

        ttk.Label(self.dest_sp, text="Library").grid(row=1, column=0, sticky="w")
        self.dest_lib_combo = ttk.Combobox(self.dest_sp, textvariable=self.dest_lib_name_var, width=COMBO_W, state="readonly")
        self.dest_lib_combo.grid(row=1, column=1, sticky="w", padx=6, pady=2)

        ttk.Label(self.dest_sp, text="Parent folder").grid(row=2, column=0, sticky="w")
        self.dest_parent_combo = ttk.Combobox(self.dest_sp, textvariable=self.dest_parent_name_var, width=COMBO_W, state="readonly")
        self.dest_parent_combo.grid(row=2, column=1, sticky="w", padx=6, pady=2)

        # Destination: OneDrive frame (fixed widths)
        self.dest_od = ttk.Frame(right)
        ttk.Label(self.dest_od, text="User search").grid(row=0, column=0, sticky="w")
        self.dest_user_entry = ttk.Entry(self.dest_od, textvariable=self.dest_user_query_var, width=COMBO_W)
        self.dest_user_entry.grid(row=0, column=1, sticky="w", padx=6, pady=2)
        ttk.Button(self.dest_od, text="Find users", command=self.on_search_users)\
            .grid(row=0, column=2, padx=6)
        self.dest_user_combo = ttk.Combobox(self.dest_od, textvariable=self.dest_user_name_var, width=COMBO_W, state="readonly")
        self.dest_user_combo.grid(row=1, column=1, sticky="w", padx=6, pady=2)

        # Show correct destination sub-frame
        self.toggle_dest_mode()

        # Binders
        self.src_site_combo.bind("<<ComboboxSelected>>", self.on_src_site_selected)
        self.src_lib_combo.bind("<<ComboboxSelected>>", self.on_src_lib_selected)
        self.src_parent_combo.bind("<<ComboboxSelected>>", self.on_src_parent_chosen)
        self.dest_site_combo.bind("<<ComboboxSelected>>", self.on_dest_site_selected)
        self.dest_lib_combo.bind("<<ComboboxSelected>>", self.on_dest_lib_selected)
        self.dest_parent_combo.bind("<<ComboboxSelected>>", self.on_dest_parent_chosen)
        self.dest_user_combo.bind("<<ComboboxSelected>>", self.on_dest_user_chosen)
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
        # bound in entry via src_parent_var
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
        # value is already bound via dest_parent_var
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
        # show or hide 
        for f in (self.dest_sp, self.dest_od):
            f.grid_forget()
        if self.dst_type_var.get() == "sp":
            self.dest_sp.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(4,0))
        else:
            self.dest_od.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(4,0))

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
