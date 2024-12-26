import tkinter as tk
from tkinter import messagebox
from tkinter.ttk import Notebook, Treeview, Scrollbar, Style
from pymongo import MongoClient
import threading
from bson.json_util import dumps, loads
import redis
import json
from hdfs import InsecureClient
from PIL import Image, ImageTk
from io import BytesIO
import cv2
import numpy as np
import webbrowser
import os
from datetime import datetime
from bson import ObjectId



class MongoDBGUIApp:
    def __init__(self, root):
        self.root = root
        self.root.title("MongoDB GUI Application")
        self.collections = ["user", "article", "read", "beread", "poprank"]

        # Style for Treeview headers
        style = Style()
        style.configure("Treeview.Heading", font=("Helvetica", 10, "bold"), foreground="black")

        # Database connection frame
        self.conn_frame = tk.Frame(root)
        self.conn_frame.pack(pady=10)

        tk.Label(self.conn_frame, text="Host:").grid(row=0, column=0, padx=5, pady=5)
        self.host_entry = tk.Entry(self.conn_frame)
        self.host_entry.insert(0, "localhost")
        self.host_entry.grid(row=0, column=1, padx=5, pady=5)

        tk.Label(self.conn_frame, text="Port:").grid(row=1, column=0, padx=5, pady=5)
        self.port_entry = tk.Entry(self.conn_frame)
        self.port_entry.insert(0, "30000")
        self.port_entry.grid(row=1, column=1, padx=5, pady=5)

        tk.Label(self.conn_frame, text="Database:").grid(row=2, column=0, padx=5, pady=5)
        self.db_entry = tk.Entry(self.conn_frame)
        self.db_entry.insert(0, "ddbs")
        self.db_entry.grid(row=2, column=1, padx=5, pady=5)

        tk.Button(self.conn_frame, text="Connect", command=self.connect_to_db).grid(row=3, columnspan=2, pady=10)

        # Tab control
        self.notebook = Notebook(root)
        self.notebook.pack(expand=1, fill="both", pady=10)

        # Dictionary to hold the Treeview widgets for each collection
        self.tabs = {}
        
        # Add a button to query data
        self.query_button = tk.Button(root, text="Query Data", command=self.query_data)
        self.query_button.pack(pady=10)

         # Add a button to add an entry below the tab control
        self.add_entry_button = tk.Button(root, text="Add Entry", command=self.add_entry)
        self.add_entry_button.pack(pady=10)

        # Add a button to update an entry
        self.update_button = tk.Button(root, text="Update Entry", command=self.update_entry)
        self.update_button.pack(pady=10)
        
        # Add Delete Entry button
        self.delete_button = tk.Button(root, text="Delete Entry", command=self.delete_entry)
        self.delete_button.pack(pady=10)

        # Add a button to monitor the entire cluster
        self.monitor_cluster_button = tk.Button(root, text="Monitor DBMS Cluster", command=self.monitor_cluster)
        self.monitor_cluster_button.pack(pady=10)

        self.monitor_hdfs_button = tk.Button(root, text="Monitor HDFS", command=lambda: self.open_webbrowser('http://localhost:9870/'))
        self.monitor_hdfs_button.pack(pady=10)
        
        # Initialize Redis
        self.init_redis()

        self.shard_keys = {
            "user": {"region", "uid"},
            "article": {"category", "aid"},
            "read": {"region", "id"}
        }
        
        self.beread_numerical_columns = {"_id", "readNum", "commentNum", "agreeNum", "shareNum"}


    def open_webbrowser(self, url):
        """
        Open the specified URL in the default web browser.
        """
        try:
            webbrowser.open(url)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to open web browser: {e}")

    def init_redis(self):
        try:
            self.redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True, password="sqq24")
            self.redis_client.ping()
            print("Connected to Redis.")
        except redis.ConnectionError as e:
            print(f"Error connecting to Redis: {e}")


    # Inside the MongoDBGUIApp class
    def start_article_monitoring(self):
        """
        Start monitoring the article collection for changes
        and aggregate data to articlesci.
        """
        try:
            # Start a new thread for monitoring
            threading.Thread(target=self.monitor_article_changes, daemon=True).start()
            print("Started monitoring changes in the 'article' collection.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to start monitoring: {e}")

    def monitor_article_changes(self):
        """
        Monitor the 'article' collection for changes and update
        the 'articlesci' collection.
        """
        try:
            with self.db.article.watch([{'$match': {'fullDocument.category': 'science'}}]) as stream:
                for change in stream:
                    # print("Change detected:", dumps(change, indent=2))

                    # Aggregate and update 'articlesci' collection
                    self.db.article.aggregate([
                        {"$match": {"category": "science"}},
                        {"$merge": {"into": "articlesci", "whenMatched": "replace"}}
                    ])

                    # Refresh the 'articlesci' tab in the GUI (if it exists)
                    if "article" in self.tabs:
                        tree, columns = self.tabs.get("article", (None, None))
                        tree.delete(*tree.get_children())
                        self.populate_data(tree, self.db["article"], columns, 'article')
                print("Successfully updated article.")
        except Exception as e:
            print(f"Error while monitoring changes: {e}")

    def start_read_monitoring(self):
        """
        Start monitoring the read collection for changes.
        """
        try:
            # Start a new thread for monitoring
            threading.Thread(target=self.monitor_read_changes, daemon=True).start()
            print("Started monitoring changes in the 'read' collection.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to start monitoring: {e}")

    def monitor_read_changes(self):
        """
        Monitor the 'read' collection for changes and update
        related collections (beread, popRank, etc.).
        """
        try:
            with self.db.read.watch([{'$match': {'operationType': 'insert'}}]) as stream:
                for change in stream:
                    change_doc = change['fullDocument']
                    print("Change detected:", dumps(change_doc, indent=2))

                    # Process the change to update other collections
                    self.update_beread(change_doc)
                    self.update_popularity_scores(change_doc)

        except Exception as e:
            print(f"Error while monitoring read changes: {e}")

    def update_beread(self, change):
        """
        Update the beread and bereadsci collections based on the change document.
        """
        try:
            # Fetch the existing document
            doc = self.db.beread.find_one({"_id": change['aid']})
            # for key, value in doc.items():
            #     print(f"{key}: {value}")
            if not doc:
                return
            # print(change)
            # Update user interaction lists
            interaction_keys = ['readOrNot', 'commentOrNot', 'agreeOrNot', 'shareOrNot']
            uid_lists = {key: doc.get(f"{key[:-3]}UidList", []) for key in interaction_keys}

            for key in interaction_keys:
                if int(change[key]) > 0 and change['uid'] not in uid_lists[key]:
                    uid_lists[key].append(change['uid'])

            # Create the updated document
            beread_dict = {
                "_id": doc['_id'],
                "category": doc['category'],
                "timestamp": doc['timestamp'],
                "readNum": int(doc['readNum']) + int(change['readOrNot']),
                "readUidList": uid_lists['readOrNot'] + doc["readUidList"],
                "commentNum": int(doc['commentNum']) + int(change['commentOrNot']),
                "commentUidList": uid_lists['commentOrNot'] + doc["commentUidList"],
                "agreeNum": int(doc['agreeNum']) + int(change['agreeOrNot']),
                "agreeUidList": uid_lists['agreeOrNot'] + doc['agreeUidList'],
                "shareNum": int(doc['shareNum']) + int(change['shareOrNot']),
                "shareUidList": uid_lists['shareOrNot'] + doc['shareUidList'],
                "aid": doc['aid'],
            }

            # Update the beread and bereadsci collections
            self.db.beread.replace_one({"_id": change['aid']}, beread_dict)
            if change['category'] == "science":
                self.db.bereadsci.replace_one({"_id": change['aid']}, beread_dict)

            # Update the GUI with the new beread and bereadsci data
            if "beread" in self.tabs:
                
                tree, columns = self.tabs.get("beread", (None, None))
                tree.delete(*tree.get_children())  # Clear Treeview
                self.populate_data(tree, self.db["beread"], columns, 'beread')

            print("Successfully updated beread.")

        except Exception as e:
            print(f"Error updating beread: {e}")

    def update_popularity_scores(self, change):
        """
        Update the popRank, popRankSci, and popRankTech collections based on the change document.
        """
        try:
            # Extract timestamp details
            timestamp = int(change['timestamp'][:-3])
            date_change = datetime.fromtimestamp(timestamp)
            year_change, month_change, week_change, day_change = (
                date_change.year,
                date_change.month,
                date_change.isocalendar()[1],
                date_change.timetuple().tm_yday,
            )

            # Fetch all relevant documents in the same month as the change document
            cursor_docs = self.db.read.find({
                "$and": [
                    {"$expr": {"$eq": [{"$month": {"$toDate": {"$toLong": "$timestamp"}}}, month_change]}},
                    {"$expr": {"$eq": [{"$year": {"$toDate": {"$toLong": "$timestamp"}}}, year_change]}}
                ]
            })

            # Initialize popularity score dictionaries
            art_pop_scores = {"monthly": {}, "weekly": {}, "daily": {}}

            for doc in cursor_docs:
                aid = doc['aid']
                date_doc = datetime.fromtimestamp(int(doc['timestamp'][:-3]))
                wk = date_doc.isocalendar()[1]
                day = date_doc.timetuple().tm_yday

                # Calculate scores for each granularity
                score = sum(int(doc.get(key, 0)) for key in ['readOrNot', 'commentOrNot', 'agreeOrNot', 'shareOrNot'])
                art_pop_scores["monthly"][aid] = art_pop_scores["monthly"].get(aid, 0) + score

                if wk == week_change:
                    art_pop_scores["weekly"][aid] = art_pop_scores["weekly"].get(aid, 0) + score

                if day == day_change:
                    art_pop_scores["daily"][aid] = art_pop_scores["daily"].get(aid, 0) + score

            # Function to get the top 5 articles
            def get_top5(scores):
                return [aid for aid, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]]

            top5 = {granularity: get_top5(scores) for granularity, scores in art_pop_scores.items()}

            # Prepare and update popRank documents
            for granularity, key in [("monthly", "m"), ("weekly", "w"), ("daily", "d")]:
                pop_dict = {
                    "_id": f"{key}{timestamp}",
                    "timestamp": timestamp,
                    "articleAidList": top5[granularity],
                    "temporalGranularity": granularity,
                }
                self.db.popRank.replace_one({"_id": pop_dict["_id"]}, pop_dict, upsert=True)


            # Update the GUI with the new beread and bereadsci data
            if "poprank" in self.tabs:
                tree, columns = self.tabs.get("poprank", (None, None))
                tree.delete(*tree.get_children())  # Clear Treeview
                self.populate_data(tree, self.db["poprank"], columns, 'poprank')

            print("Successfully updated poprank.")

        except Exception as e:
            print(f"Error updating popularity scores: {e}")

            
    def monitor_cluster(self):
        # Create a monitoring window
        monitor_window = tk.Toplevel(self.root)
        monitor_window.title("Cluster Monitoring")
        HOSTIP = self.host_entry.get()

        # Scrollable Frame
        canvas = tk.Canvas(monitor_window)
        scrollbar = tk.Scrollbar(monitor_window, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        components = {
            "Config Server": {"host": f"{HOSTIP}:30001", "replica_set": "cfg"},
            "DBMS1": {"host": f"{HOSTIP}:30004", "replica_set": "dbms1"},
            "DBMS2": {"host": f"{HOSTIP}:30005", "replica_set": "dbms2"},
            "Mongos Router": {"host": f"{HOSTIP}:30000", "type": "mongos"}
        }

        for component_name, config in components.items():
            try:
                cache_key = f"monitor:{component_name}"
                cached_status = self.redis_client.get(cache_key)

                if cached_status:
                    status_text = cached_status  # Use cached status
                    print(f"Cache hit for {component_name}.")
                else:
                    # Generate live status if no cache exists
                    client = MongoClient(f"mongodb://{config['host']}")
                    server_status = client.admin.command("serverStatus")
                    print(f"Cache miss for {component_name}.")
                    # Cache the status for 30 seconds
                    status_text = f"{component_name} is live with version {server_status['version']}."
                    self.redis_client.set(cache_key, status_text, ex=30)

                # tk.Label(monitor_window, text=status_text, anchor="w").pack(pady=5, padx=10)
            except Exception as e:
                error_text = f"Component: {component_name}\nError: {str(e)}"
                print(error_text)
                # tk.Label(monitor_window, text=error_text, fg="red", anchor="w").pack(pady=5, padx=10)
            
            try:
                # Connect to the component
                client = MongoClient(f"mongodb://{config['host']}")
                server_status = client.admin.command("serverStatus")

                # Collect basic stats
                status_text = f"Component: {component_name}\n"
                status_text += f"Host: {server_status['host']}\n"
                status_text += f"Version: {server_status['version']}\n"
                status_text += f"Uptime (seconds): {server_status['uptime']}\n"
                status_text += f"Connections: {server_status['connections']['current']}\n"

                if config.get("type") == "mongos":
                    # Fetch shard information
                    shards = client.admin.command("listShards")["shards"]
                    status_text += "\nShards Info:\n"
                    status_text += f"  Shards Managed: {len(shards)}\n"
                    for shard in shards:
                        status_text += f"    {shard['_id']}: {shard['host']} (Tags: {shard['tags']})\n"

                else:
                    # Database-specific stats for shards/config servers
                    db_stats = client.get_database("admin").command("dbStats")
                    data_size_kb = db_stats['dataSize'] / 1024
                    storage_size_kb = db_stats['storageSize'] / 1024
                    status_text += f"Data Size (KB): {data_size_kb:.2f}\n"
                    status_text += f"Storage Size (KB): {storage_size_kb:.2f}\n"

                    # Workload Information
                    ops_per_sec = server_status["opcounters"]
                    status_text += "\nWorkload Info:\n"
                    status_text += f"  Insert Operations: {ops_per_sec['insert']}\n"
                    status_text += f"  Query Operations: {ops_per_sec['query']}\n"
                    status_text += f"  Update Operations: {ops_per_sec['update']}\n"
                    status_text += f"  Delete Operations: {ops_per_sec['delete']}\n"

                    # Replica Set Information
                    if config.get("replica_set"):
                        repl_status = client.admin.command("replSetGetStatus")
                        members = len(repl_status['members'])
                        primary = any(m['stateStr'] == 'PRIMARY' for m in repl_status['members'])
                        status_text += "\nReplica Set Info:\n"
                        status_text += f"  Replica Set Name: {config['replica_set']}\n"
                        status_text += f"  Members: {members}\n"
                        status_text += f"  Primary: {primary}\n"

                    # Fetch collection information from config database including number of documents
                    # import pdb; pdb.set_trace()
                    collections = client.get_database("ddbs").list_collections()
                    
                    status_text += "\nCollections Info:\n"
                    for collection in collections:
                        # Fetch only the document count
                        document_count = client.get_database("ddbs")[collection['name']].count_documents({})
                        # import pdb; pdb.set_trace()
                        status_text += f"  {collection['name']}: {document_count} documents\n"

                status_text += "-" * 50

                # Display the status in the scrollable frame with text folding
                tk.Label(scrollable_frame, text=status_text, justify="left", anchor="nw").pack(pady=5, padx=10)

            except Exception as e:
                # Handle errors for each component
                error_text = f"Component: {component_name}\nError: {str(e)}\n" + "-" * 50
                tk.Label(scrollable_frame, text=error_text, fg="red", justify="left", anchor="nw").pack(pady=5, padx=10)

    def delete_entry(self):
        # Get the currently selected tab
        current_tab = self.notebook.select()
        tab_index = self.notebook.index(current_tab)
        collection_name = self.collections[tab_index]

        # Get the Treeview and columns for the selected collection
        tree, columns = self.tabs.get(collection_name, (None, None))
        if not tree or not columns:
            messagebox.showerror("Error", "No collection selected or columns unavailable.")
            return

        if collection_name not in ["user", "article", "read"]:
            messagebox.showerror("Error", "Delete entry is only allowed on user, article, and read collections.")
            return

        # Get the selected item
        selected_item = tree.selection()
        if not selected_item:
            messagebox.showwarning("Warning", "No item selected for deletion.")
            return

        # Retrieve the document's data from the selected Treeview item
        values = tree.item(selected_item, "values")
        if not values:
            messagebox.showerror("Error", "Could not retrieve document details for deletion.")
            return
        
        # # Build the query including the shard key
        # try:
        #     query = {"_id": ObjectId(values[columns.index("_id")])}

        #     # Include shard keys in the query
        #     shard_key_values = {}
        #     for shard_key in self.shard_keys[collection_name]:
        #         if shard_key in columns:
        #             shard_key_values[shard_key] = values[columns.index(shard_key)]

        #     # Add shard key fields to the query
        #     query.update(shard_key_values)

        # except ValueError as e:
        #     messagebox.showerror("Error", f"Missing required field for deletion: {e}")
        #     return

        # Prepare the query to delete the document
        query = {columns[i]: values[i] for i in range(len(columns)) if values[i]}
        query = {"_id": ObjectId(values[0])}

        try:
            # Confirm deletion with the user
            confirm = messagebox.askyesno("Confirm Deletion", f"Are you sure you want to delete this entry?\n\n{query}")
            if not confirm:
                return

            # Delete the document from MongoDB
            collection = self.db[collection_name]
            print(query)
            result = collection.delete_one(query)
            # print(result)
            if result.deleted_count > 0:
                # Remove the item from the Treeview
                tree.delete(selected_item)
                messagebox.showinfo("Success", "Entry deleted successfully.")
            else:
                messagebox.showerror("Error", "Failed to delete entry. It might have already been removed.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete entry: {e}")

    def query_data(self):
        current_tab = self.notebook.select()
        tab_index = self.notebook.index(current_tab)
        collection_name = self.collections[tab_index]
        tree, columns = self.tabs.get(collection_name, (None, None))

        if not tree or not columns:
            messagebox.showerror("Error", "No collection selected or columns unavailable.")
            return
            
        # Create update window
        query_window = tk.Toplevel(self.root)
        query_window.title(f"Query Entry in {collection_name.capitalize()}")
        input_entries = {}

        # Create entry fields with current values, allowing empty values
        for idx, col in enumerate(columns):
            tk.Label(query_window, text=f"{col}:").grid(row=idx, column=0, padx=5, pady=5)
            entry = tk.Entry(query_window)
            entry.insert(0, '')
            entry.grid(row=idx, column=1, padx=5, pady=5)

            if collection_name == "beread" and col in self.beread_numerical_columns:
                try:
                    input_entries[col] = int(entry)
                except Exception as e:
                    messagebox.showerror("Error", f"Input is of the wrong format.")
            else:
                input_entries[col] = entry


        def execute_query():
            try:
                # Parse the query conditions

                query_text = {}
                for col, entry in input_entries.items():
                    if entry.get() == "":
                        continue
                    query_text[col] = entry.get()
                    if col == "_id" and collection_name in ['user', 'article', 'read']:
                        query_text[col] = ObjectId(query_text[col])
                
                query_text = str(query_text)
                query_text = query_text.replace("'", '"')
                query_conditions = json.loads(query_text) if query_text else {}
                # print(query_text)
                # Generate Redis cache key
                cache_key = f"{collection_name}:{query_text}"
                cached_results = self.redis_client.get(cache_key)

                if cached_results:
                    print("Cache hit!")
                    # Use `loads` from bson.json_util to deserialize MongoDB-specific types
                    results = loads(cached_results)
                else:
                    print("Cache miss, querying MongoDB.")
                    collection = self.db[collection_name]
                    cursor = collection.find(query_conditions)
                    results = [doc for doc in cursor]

                    # Serialize results with `dumps` to handle ObjectId
                    self.redis_client.set(cache_key, dumps(results), ex=1)  # Cache for 1 hour

                # Populate Treeview with the query results
                self.populate_query_results(tree, results, columns)
                query_window.destroy()
            except json.JSONDecodeError as e:
                messagebox.showerror("Error", f"Invalid JSON query: {e}")
            except Exception as e:
                messagebox.showerror("Error", f"Query execution failed: {e}")

        tk.Button(query_window, text="Execute Query", command=execute_query).grid(row=len(columns), columnspan=2, pady=10)

    def populate_query_results(self, tree, cursor, columns):
        # Clear existing data in the Treeview
        for item in tree.get_children():
            tree.delete(item)

        # Populate Treeview with query results
        for doc in cursor:
            values = [doc.get(col, "") for col in columns]
            tree.insert("", "end", values=values)


    def add_entry(self):
        # Get the currently selected tab
        current_tab = self.notebook.select()
        tab_index = self.notebook.index(current_tab)
        collection_name = self.collections[tab_index]

        # Get the Treeview and columns for the selected collection
        tree, columns = self.tabs.get(collection_name, (None, None))
        if not tree or not columns:
            messagebox.showerror("Error", "No collection selected or columns unavailable.")
            return

        if collection_name not in ["user", "article", "read"]:
            messagebox.showerror("Error", "Add entry is only allowed on user, article, and read collections.")
            return

        # Open a new window to input data
        input_window = tk.Toplevel(self.root)
        input_window.title(f"Add Entry to {collection_name.capitalize()}")
        input_entries = {}

        for idx, col in enumerate(columns):
            if col == "_id":
                continue
            tk.Label(input_window, text=f"{col}:").grid(row=idx, column=0, padx=5, pady=5)
            entry = tk.Entry(input_window)
            entry.grid(row=idx, column=1, padx=5, pady=5)
            input_entries[col] = entry

        def submit_entry():
            # Collect input data
            data = {}
            for col, entry in input_entries.items():
                data[col] = entry.get()

            data["_id"] = ObjectId()

            try:
                # Insert data into MongoDB
                collection = self.db[collection_name]
                
                result=collection.insert_one(data)
                if not result.acknowledged:
                    messagebox.showinfo("Info", "No changes were made to the database.")
                else:
                    # Refresh Treeview data
                    tree.delete(*tree.get_children())  # Clear Treeview
                    self.populate_data(tree, collection, columns, collection_name)
                    messagebox.showinfo("Success", "Entry added successfully.")
                    input_window.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to add entry: {e}")

        tk.Button(input_window, text="Submit", command=submit_entry).grid(row=len(columns), columnspan=2, pady=10)

    def update_entry(self):
        # Get the currently selected tab and collection
        current_tab = self.notebook.select()
        tab_index = self.notebook.index(current_tab)
        collection_name = self.collections[tab_index]

        # Get the Treeview and columns for the selected collection
        tree, columns = self.tabs.get(collection_name, (None, None))
        if not tree or not columns:
            messagebox.showerror("Error", "No collection selected or columns unavailable.")
            return
        
        if collection_name not in ["user", "article", "read"]:
            messagebox.showerror("Error", "Update entry is only allowed on user, article, and read collections.")
            return

        # Get selected item
        selected_items = tree.selection()
        if not selected_items:
            messagebox.showwarning("Warning", "Please select an entry to update.")
            return

        # Get current values of selected item
        current_values = tree.item(selected_items[0])['values']
        
        # Create update window
        update_window = tk.Toplevel(self.root)
        update_window.title(f"Update Entry in {collection_name.capitalize()}")
        input_entries = {}

        # Create entry fields with current values
        for idx, (col, value) in enumerate(zip(columns, current_values)):
            if col == "_id":
                continue
            tk.Label(update_window, text=f"{col}:").grid(row=idx, column=0, padx=5, pady=5)
            entry = tk.Entry(update_window)
            entry.insert(0, str(value))
            entry.grid(row=idx, column=1, padx=5, pady=5)
            input_entries[col] = entry

        def submit_update():
            # Collect updated data
            update_data = {}
            original_data = dict(zip(columns, current_values))
            is_shard_key = False

            for col, entry in input_entries.items():
                new_value = entry.get()
                if new_value != str(original_data[col]):
                    update_data[col] = new_value
                    if col in self.shard_keys[collection_name]:
                        is_shard_key = True

            if not update_data:
                messagebox.showinfo("Info", "No changes detected.")
                update_window.destroy()
                return

            try:
                collection = self.db[collection_name]

                if is_shard_key:
                    # Shard key changed: delete the existing document and insert a new one
                    original_query = {"_id": ObjectId(original_data["_id"])}
                    result = collection.delete_one(original_query)
                    if result.deleted_count > 0:
                        # Remove the item from the Treeview
                        tree.delete(selected_items)
                    else:
                        messagebox.showerror("Error", "Failed to delete entry. It might have already been removed.")
                        return
                    new_entry = {**original_data, **update_data}
                    new_entry["_id"] = ObjectId() # need to update objectid otherwise it doesn't work
                    print("original_data", original_data)
                    print("update_data", update_data)
                    print("new data", new_entry)
                    result = collection.insert_one(new_entry)

                    if not result.acknowledged:
                        messagebox.showinfo("Info", "No changes were made to the database.")
                    else:
                        # Refresh Treeview data
                        tree.delete(*tree.get_children())  # Clear Treeview
                        self.populate_data(tree, collection, columns, collection_name)
                        messagebox.showinfo("Success", "Shard key updated and entry relocated successfully.")

                else:
                    # Shard key unchanged: perform a standard update
                    original_query = {"_id": ObjectId(original_data["_id"])}
                    result = collection.update_one(original_query, {"$set": update_data})
                    if result.modified_count == 0:
                        messagebox.showinfo("Info", "No changes were made to the database.")
                    else:
                        messagebox.showinfo("Success", "Entry updated successfully.")
                
                # Refresh Treeview data
                tree.delete(*tree.get_children())  # Clear Treeview
                self.populate_data(tree, collection, columns, collection_name)
                update_window.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to update entry: {e}")

        tk.Button(update_window, text="Update", command=submit_update).grid(row=len(columns), columnspan=2, pady=10)

    def connect_to_db(self):
        if hasattr(self, 'db'):
            messagebox.showinfo("Info", "Already connected to a database.")
            return
        host = self.host_entry.get()
        port = self.port_entry.get()
        db_name = self.db_entry.get()
        try:
            client = MongoClient(f"mongodb://{host}:{port}")
            self.db = client[db_name]
            messagebox.showinfo("Success", f"Connected to database '{db_name}'")
            self.populate_tabs()
            self.start_article_monitoring()
            self.start_read_monitoring()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to connect: {e}")

    def populate_tabs(self):
        for collection_name in self.collections:
            frame = tk.Frame(self.notebook)
            self.notebook.add(frame, text=collection_name.capitalize())

            # Fetch a sample document to determine columns
            try:
                collection = self.db[collection_name]
                sample_doc = collection.find_one()
                if not sample_doc:
                    # If the collection is empty, show a message
                    label = tk.Label(frame, text="No data found in this collection.")
                    label.pack(pady=20)
                    continue

                # Dynamically generate columns based on the keys of the sample document
                columns = list(sample_doc.keys())

                # Create Treeview for the collection with fixed dimensions
                tree_frame = tk.Frame(frame)
                tree_frame.pack(expand=1, fill="both", padx=5, pady=5)

                tree = Treeview(tree_frame, columns=columns, show="headings", height=15, style="Treeview")
                tree.pack(side="left", fill="both", expand=True)

                # Configure columns
                for col in columns:
                    tree.heading(col, text=col)
                    tree.column(col, anchor="center", width=150)  # Fixed width for each column

                # Add vertical scrollbar
                vsb = Scrollbar(tree_frame, orient="vertical", command=tree.yview)
                tree.configure(yscrollcommand=vsb.set)
                vsb.pack(side="right", fill="y")

                # Add horizontal scrollbar
                hsb = Scrollbar(frame, orient="horizontal", command=tree.xview)
                tree.configure(xscrollcommand=hsb.set)
                hsb.pack(side="bottom", fill="x")

                # Add the Treeview to the tabs dictionary
                self.tabs[collection_name] = (tree, columns)

                # Populate data into the Treeview
                self.populate_data(tree, collection, columns, collection_name)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to create tab for {collection_name}: {e}")

    def populate_data(self, tree, collection, columns, collection_name):
        # Clear any existing data
        for item in tree.get_children():
            tree.delete(item)

        # Insert documents into the Treeview, sorted by _id
        if collection_name in ["user", "article", "read"]:
            collection_load = collection.find().sort("id")
        else:
            collection_load = collection.find()
        for doc in collection_load:
            values = [doc.get(col, "") for col in columns]
            tree.insert("", "end", values=values)


if __name__ == "__main__":
    root = tk.Tk()
    app = MongoDBGUIApp(root)
    root.geometry("1000x1000")  # Fixed window size
    root.mainloop()
