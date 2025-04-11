import re
import importlib.util
import glob
import os

# function to change the vertical names to the required format
def change_vertical_name(vertical):
    
    # replace the & with "_" and "," with "_"
    vertical = re.sub(r' ', '', vertical)
    vertical = re.sub(r'&', 'And', vertical)
    vertical = re.sub(r',', '_', vertical)

    return vertical

def convert_to_snake_casing(vertical):
    
    # replace the & with "_" and "," with "_"
    vertical = vertical.lower()
    vertical = re.sub(r'&', 'and', vertical)
    vertical = re.sub(r' ', '_', vertical)
    vertical = re.sub(r',', '_', vertical)

    return vertical

  
def get_all_jiomart_legos_confs():
    files = sorted(glob.glob("workflow/dags/jiomart_legos_*_conf.py"))

    config_dict = {}

    for file in files:
        # Extract the vertical name (e.g., "groceries" from "jiomart_legos_groceries_conf.py")
        filename = os.path.basename(file)
        key = filename.replace("jiomart_legos_", "").replace("_conf.py", "")

        # Load the module dynamically
        spec = importlib.util.spec_from_file_location(f"module_{key}", file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Assume the dict variable inside the file is named `task_configs`
        if hasattr(module, "task_configs"):
            config_dict[key] = module.task_configs  # Store the dictionary in our result

    print(config_dict)  # The final dictionary containing all configs
    return config_dict
