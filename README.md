# Gotlin

Gotlin is a distributed computing framework, which is similar to Apache MapReduce. It is a lightweight and customizable computing framework specially designed for Golang.
It is divided into service nodes, computing nodes and clients. The client submits tasks to the service nodes, and the tasks are divided into multiple instructions and form a directed acyclic graph according to the dependencies of the instructions. The service node will schedule the instructions to the computing node, and when all the instructions are calculated, the service node will return the result of the task to the client. At this point, a task ends.
The gotlin framework has a rich set of built-in instructions, including basic arithmetic instructions, logical instructions, data instructions [immediate values, database input and output], collection instructions [intersection, union, difference], table instructions [join, union, filter, grouping, sorting]. You can also customize your own instruction, register the computing node that can process the instruction to the service node, and the instruction can participate in the calculation. Hope you enjoy it.

*\*Important: The project is still in development, do not use in production environment.*

## Installation

Use go get.

```sh
$ go get -u github.com/yaoguais/gotlin
```

## Quick start

Start gotlin server.

```
$ gotlin start
```

## Architecture

[![Gotlin Architecture Diagram](./images/gotlin_architecture_diagram.png)](./images/gotlin_architecture_diagram.png)

### License

    Copyright 2013 Mir Ikram Uddin

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
