# Create a diagram similar to the uploaded image

from matplotlib.patches import FancyArrowPatch, Ellipse, Rectangle
import matplotlib.pyplot as plt
from matplotlib.patches import Arc



# Improved visual style for the diagram
fig, ax = plt.subplots(figsize=(10, 6))

# User App
ellipse_user = Ellipse((0.2, 0.8), width=0.15, height=0.1, facecolor='gold', edgecolor='black', lw=2)
ax.add_patch(ellipse_user)
ax.text(0.2, 0.8, "User App", ha="center", va="center", fontsize=12, fontweight="bold")

# Mongos Router
rect_mongos = Rectangle((0.4, 0.7), width=0.2, height=0.15, facecolor='limegreen', edgecolor='black', lw=2)
ax.add_patch(rect_mongos)
ax.text(0.5, 0.775, "Mongos\n(router)", ha="center", va="center", fontsize=12, fontweight="bold", color="white")

# Config Servers
rect_cfg1 = Rectangle((0.75, 0.85), width=0.15, height=0.07, facecolor='mediumseagreen', edgecolor='black', lw=1.5)
ax.add_patch(rect_cfg1)
ax.text(0.825, 0.885, "P\ncfgsvr1", ha="center", va="center", fontsize=10, fontweight="bold")

rect_cfg2 = Rectangle((0.75, 0.77), width=0.15, height=0.07, facecolor='mediumseagreen', edgecolor='black', lw=1.5)
ax.add_patch(rect_cfg2)
ax.text(0.825, 0.805, "S\ncfgsvr2", ha="center", va="center", fontsize=10, fontweight="bold")

rect_cfg3 = Rectangle((0.75, 0.69), width=0.15, height=0.07, facecolor='mediumseagreen', edgecolor='black', lw=1.5)
ax.add_patch(rect_cfg3)
ax.text(0.825, 0.725, "S\ncfgsvr3", ha="center", va="center", fontsize=10, fontweight="bold")

# Config Servers Group
ax.add_patch(Rectangle((0.7, 0.67), width=0.25, height=0.27, fill=False, linestyle='dotted', edgecolor='black', lw=1.5))
ax.text(0.825, 0.955, "Config Servers", fontsize=12, fontweight="bold", ha="center", color="darkgreen")

# DBMS Servers
rect_dbms1 = Rectangle((0.75, 0.5), width=0.15, height=0.07, facecolor='coral', edgecolor='black', lw=1.5)
ax.add_patch(rect_dbms1)
ax.text(0.825, 0.535, "dbms1", ha="center", va="center", fontsize=10, fontweight="bold")

rect_dbms2 = Rectangle((0.75, 0.42), width=0.15, height=0.07, facecolor='coral', edgecolor='black', lw=1.5)
ax.add_patch(rect_dbms2)
ax.text(0.825, 0.455, "dbms2", ha="center", va="center", fontsize=10, fontweight="bold")

# DBMS and GridFS Group
ax.add_patch(Rectangle((0.7, 0.40), width=0.25, height=0.20, fill=False, linestyle='dotted', edgecolor='black', lw=1.5))
ax.text(0.825, 0.61, "DBMS", fontsize=12, fontweight="bold", ha="center", color="darkred")

# Redis Server
rect_redis = Rectangle((0.45, 0.30), width=0.2, height=0.15, facecolor='orange', edgecolor='black', lw=2)
ax.add_patch(rect_redis)
ax.text(0.55, 0.375, "Redis", ha="center", va="center", fontsize=12, fontweight="bold")

# Hadoop Components (Namenode, Datanode, ResourceManager, NodeManager, HistoryServer)
# Center the Hadoop components by adjusting their Y positions
center_x = 0.25  # Horizontal centering for all components

# Namenode
rect_namenode = Rectangle((center_x - 0.075, 0.5), width=0.15, height=0.07, facecolor='deepskyblue', edgecolor='black', lw=1.5)
ax.add_patch(rect_namenode)
ax.text(center_x, 0.535, "Namenode", ha="center", va="center", fontsize=10, fontweight="bold")

# Datanode
rect_datanode = Rectangle((center_x - 0.075, 0.42), width=0.15, height=0.07, facecolor='deepskyblue', edgecolor='black', lw=1.5)
ax.add_patch(rect_datanode)
ax.text(center_x, 0.455, "Datanode", ha="center", va="center", fontsize=10, fontweight="bold")

# ResourceManager
rect_resourcemanager = Rectangle((center_x - 0.075, 0.31), width=0.15, height=0.07, facecolor='deepskyblue', edgecolor='black', lw=1.5)
ax.add_patch(rect_resourcemanager)
ax.text(center_x, 0.345, "ResourceManager", ha="center", va="center", fontsize=10, fontweight="bold")

# NodeManager
rect_nodemanager = Rectangle((center_x - 0.075, 0.23), width=0.15, height=0.07, facecolor='deepskyblue', edgecolor='black', lw=1.5)
ax.add_patch(rect_nodemanager)
ax.text(center_x, 0.265, "NodeManager", ha="center", va="center", fontsize=10, fontweight="bold")

# HistoryServer
rect_historyserver = Rectangle((center_x - 0.075, 0.14), width=0.15, height=0.07, facecolor='deepskyblue', edgecolor='black', lw=1.5)
ax.add_patch(rect_historyserver)
ax.text(center_x, 0.175, "HistoryServer", ha="center", va="center", fontsize=10, fontweight="bold")

# Hadoop Components Group
ax.add_patch(Rectangle((center_x - 0.1, 0.11), width=0.20, height=0.48, fill=False, linestyle='dotted', edgecolor='black', lw=1.5))
ax.text(center_x, 0.595, "HDFS", fontsize=12, fontweight="bold", ha="center", color="darkblue")


# Arrows
arrow_user_to_mongos = FancyArrowPatch((0.275, 0.8), (0.4, 0.775), arrowstyle='-|>', mutation_scale=15, color='blue', lw=2)
ax.add_patch(arrow_user_to_mongos)

arrow_mongos_to_cfg = FancyArrowPatch((0.6, 0.775), (0.7, 0.825), arrowstyle='-|>', mutation_scale=15, color='blue', lw=2)
ax.add_patch(arrow_mongos_to_cfg)

arrow_mongos_to_dbms = FancyArrowPatch((0.6, 0.725), (0.7, 0.515), arrowstyle='-|>', mutation_scale=15, color='blue', lw=2)
ax.add_patch(arrow_mongos_to_dbms)

arrow_user_to_redis = FancyArrowPatch((0.275, 0.8), (0.45, 0.375), arrowstyle='-|>', mutation_scale=15, color='blue', lw=2)
ax.add_patch(arrow_user_to_redis)

arrow_user_to_hdfs = FancyArrowPatch((0.275, 0.8), (0.20, 0.585), arrowstyle='-|>', mutation_scale=15, color='blue', lw=2)
ax.add_patch(arrow_user_to_hdfs)

arrow_cfg_to_dbms_gridfs = FancyArrowPatch(
    (0.95, 0.67),  # Start from the right of the Config Servers box
    (0.95, 0.5),   # End at the right of the DBMS Servers box
    arrowstyle='<|-|>', 
    mutation_scale=15, 
    color='blue', 
    lw=2,
    connectionstyle="arc3,rad=-0.5"
)
ax.add_patch(arrow_cfg_to_dbms_gridfs)

# Set limits and hide axes
ax.set_xlim(0, 1)
ax.set_ylim(0, 1)
ax.axis('off')

# Display the diagram
# plt.title("System Architecture", fontsize=16, fontweight="bold", color="navy")
plt.savefig('system_architecture_diagram.png')

