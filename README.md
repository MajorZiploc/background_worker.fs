# TODO:

Implement exponential backoff for rety on failing tasks

NOTE: this maybe be difficult. a simplier approach might be to fail the failed task and insert a new QUEUED task with retry decremented. this way, a task wont block the rest of the tasks
