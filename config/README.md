# Understanding the Configuration Files
## Purpose and Motivation
Each configuration file serves a specific purpose that will be covered in their own sections. As such, this section will be covering the design chocies made when creating this system of configuration files.

These files were created to give the end-user some agency in configuring the pipeline and allow for 'default' configurations. By exposing only certain aspects of the overall pipeline to the user, this also allows for the rest of the pipeline to remain undisturbed by well-meaning but potentially careless users.

The rationale that lead to the use of several JSON files was two-fold. First, as the end-user is not expected to have a deep understanding of how programming works, it is unreasonable to make use of configuration files where the formatting is not immediately intuitive. JSON as a file structure, is somewhat approachable in this regard, as it is very easy to make simple adjustments to values and can be understood even without a deep understanding of how the program makes use of it. Second, Python already has a built-in library `json` that handles parsing data to and from a JSON structure. As such, it was considered the best choice for the task.

---

## How the Files are Used
The JSON files are used by the pipeline through the ConfigurationManager class, which reads in the necessary portions of each file (or the whole file) and uses it to determine different aspects of each Submission, allowing for granular control in the overall pipeline.
## Configuration Files
This section will be giving a short statement on the purpose of each configuration file individually.
### Pipeline Configuration
This configuration file is meant to serve as the default profile for the pipeline configuration. In other words, these profiles dictate how the pipeline is going to be constructed. This allows for more advanced users to dictate the order in which things are processed, alongside what process steps will occur for any given thing that goes through the pipeline.
### Submission Start Pipeline Tags
This file is a TAG repository for submissions that need placement at a non-standard starting point. If a partially finished submission, or one that only needs uploading, for example, are tagged accordingly, this file ensures that those submissions are placed in the appropriate queue.

---

## Adding New Configuration Files
If one wishes to add new configuration files to the system, then the following needs to be done to properly hook it into the pipeline.

1. Create an attribute inside of ConfigurationManager that houses the parsed JSON file. If the JSON file has different structures dependant on some meta-structure (e.g., machine type, etc.), then loading the JSON is best performed in the driver program. A basic example is `self.example_json = self.load_json(json_file_name="new_config.json")`
2. Create `getter` functions for accessing either the entire data structure or portions of it. For examples of this, see the `fetch_from` and `get_` functions in `core/ConfigurationManager.py`
3. [OPTIONAL] Create `determine` functions that provide some information based on other pieces of information from the pipeline. For examples of this, see the `determine_` functions in `core/ConfigurationManager.py`