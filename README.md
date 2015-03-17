# AsynCopy
Description:

  This is a prototype of multi-threaded cp program. However, unlike the traditional single-threaded cp, this new cp program, Asynchronous CP, aka acp, utilizes the combination of multi-threading and asynchronous IO. ACP will visit a directory tree in Breath First Order, assign each sub-directory to a new thread and then do a flat copy, e.g., only copy the leaf node (normal files) in the directory. Instead of using read/write, ACP used Asynchronous IO to enhance the degree of parallism.
  
Benchmark Results:

  https://docs.google.com/document/d/1EyAeQixdsElDMLRN5ujpxPal7rob_-AEx_W3nG00HPI/edit?usp=sharing
  
  https://docs.google.com/document/d/1znB1PFvVVKLGvZFudOqtdw1cqNahcEmHb9il22yMi2o/edit?usp=sharing
