# Understanding the Core Files
## Purpose and Motivation
The Core files, named for their integral part in the functionality of the overall pipeline, were made with the idea of reusability and modularity in mind. By creating custom classes that encapsulate key parts of the pipeline, it is possible to continue expanding upon existing features and even integrate new features without hindering the existing pipeline.

---

## How the Files are Used
As core classes are designated by their importance for running the pipeline, most of these files are used throughout the pipeline system, or even by other core classes. For example, the `PipelineManager` class is used in the driver program as it is the main building block of the conceptual pipeline. Other classes, such as `Task` represent the basic structure by which new units of data can be handled by the pipeline.

Other core classes, however, represent features that work in tandem with the pipeline. `SlackBot`, an encapsulation of a Slack Bot functionality to bridge the information gap between end-users and the pipeline is an example of this type of core class.

---

## Core Classes
This section will cover in brief detail the purpose of each core class currently in use, along with how their purpose is achieved with respect to the pipeline itself.
### PipelineManager
As noted before, the `PipelineManager` is the lifeblood of the entire pipeline. With various Queue structures and use of multiprocessing, it manages the flow of various pieces of information and ensures that everything ends up where it is supposed to. From inter-manager communication and logging to feeding new information to a Slack Bot, it is not unreasonable to state that this is the crux of it all. It's use in the pipeline is literally being the blocks by which any given pipeline is constructed.
### Task
Similar to `PipelineManager`, the `Task` is the main unit of information that is processed in the pipeline. It's built in such a way that any variant classes will always have the necessary information for processing. As such, its role with respect ot the pipeline is serving as a mould by which new developers can create their own unit of processable data while maintaining guarantees currently present.
### SlackBot
`SlackBot` is different in that the relationship it has with the pipeline is more tangential, serving instead to provide easy-to-access information to the end-users who may not have the luxury of watching the server or computer that is running the pipeline. Different from the regular Webhooks-based notification system, `SlackBot` is built around 'ad-hoc' calls for real-time information. As such, it is constantly fed updates from all `PipelineManagers` and stores said information to be utilized at any time. Due to this, `SlackBot` is delegated to run in a background thread, meaning its ability to communicate to the user is directly tied to the health of the pipeline.

---

## Adding New Core Classes
While new core classes can be added the same as any regular custom class in Python, it is important to consider what differentiates these core classes from a regular class or portion of code. Below are a list of considerations when deciding if a new piece of code is meant to be here.

1. Is this new feature integral to running the pipeline? If the answer is yes, then there's good reason for it to be in the core classes.
2. Is this new feature going to be used extensively throughout the code? If only a few of its functions fall in this group, it is best left in the `utils` section of code. If however, the entire class must be used to gain the full benefit, it may belong here instead.
3. Does this new feature expand upon the existing capabilities of the pipeline? Even if a feature has no direct impact on the ability to run the pipeline, classes that directly expand what can be done with or using the pipeline should be considered as a core class. A good example of this is the `SlackBot` class.

If any of these considerations hold true, then there is good reason to try and construct a new core class for the pipeline.