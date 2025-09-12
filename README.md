# Kamera

**Note:** This project is a research artifact and is under active development. Its APIs and functionalities are subject to change and it is not yet recommended for production use.

`kamera` is a toolkit for observing, analyzing, and verifying the behavior of the Kubernetes control plane. It is designed specifically for control plane components (controllers) built with the `controller-runtime` framework, providing targeted instrumentation to capture the behaviors of individual controllers and the interactions between them.

The primary goal of Kamera is to help platform developers understand complex interactions within the control plane by capturing detailed execution traces, enabling offline analysis and functional replay. To ensure control plane reliability, Kamera also employs implementation-level model checking and simulation testing of control plane deployments, enabling developers to proactively catch problematic behaviors that only manifest under certain conditions before deploying their code.

## Core Capabilities

Kamera provides a set of tools to:

*   **Trace Generation:** Instrument `controller-runtime` based controllers to capture fine-grained execution traces in a minimally invasive manner.
*   **System Replay:** Replay captured scenarios to reproduce and debug issues in the control plane.
*   **Trace Analysis:** Analyze traces to understand the causal relationships between controller reconciliations  and how data consistency affects reconciliation outcomes.
*   **Simulation Testing:** Systematically explore the state space of possible reconciliation executions to verify control plane convergence properties under different event orderings and data consistency scenarios.


## Getting Started

*(To be added)*

## License

This project is licensed under the terms of the [MIT License](LICENSE).