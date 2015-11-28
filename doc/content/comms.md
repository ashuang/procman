Procman communications{#procman_comms}
============

[TOC]

# Overview {#procman_comms_overview}

Sheriffs and deputies communicate by transmitting
LCM messages to each other.  Together, the LCM
messages define the Procman communications protocol.  LCM
(http://lcm-proj.github.io) is a publish/subscribe message passing system that
typically uses UDP multicast as its underlying transport.  It may be configured
to use other types of transport as well, such as TCP.

Deputies periodically (1 Hz) transmit their current status, which includes:
- A listing of each command managed by the deputy and. For each command:
  - Whether the command is running.
  - The OS-assigned process ID of the command, if it's running.
  - How much CPU and memory are used by the command.
  - Which group the command is in.

In addition to the deputy state, each deputy also captures the standard output
and standard error for each running command, and transmits the output over LCM.

Sheriffs transmit the desired state for each deputy.  For each deputy, the
sheriff periodically (1 Hz) transmits a message containing:
- A listing of all commands the deputy _should_ be managing and the desired
state for each command:
  - If the command should be running, stopped, or restarted
  - The program to run, command line arguments, and environment variables.
  - Which group the command should be in.
  - If the command should be automatically restarted if/when it terminates.

The communications protocol is stateless by design:  every message transmitted
from a sheriff to the deputy contains the entire desired state of the deputy.
Similarly, every message transmitted by a deputy contains its entire internal
state.  This stateless protocol has a few key features:
- The entire system is robust to communication dropouts.
If a deputy stops receiving messages from the sheriff, the deputy simply
continues carrying out its last orders.
- When the sheriff starts up, it seamlessly picks up the entire state of the
system as it receives updates from each deputy.  Deputies are
unaffected by a sheriff starting up until the sheriff begins transmitting
orders.
- A sheriff in observer mode can observe the state of the system
simply by receiving deputy messages and not transmitting anything.

The following sections describe the messages transmitted and received by
deputies and sheriffs in more detail.

# Deputy {#procman_comms_deputy}

## Messages transmitted by deputy {#procman_comms_deputy_transmitted}

The deputy transmits the following messages:

LCM Channel | Message type | Description
------------|--------------|-------------
`PM_INFO` | [deputy_info_t](\ref procman_lcm_deputy_info_t) | Summarizes the state of all commands managed by a deputy.  Transmitted at 1 Hz or when a command status changes.
`PM_OUTPUT` | [output_t](\ref procman_lcm_output_t) | Published when a hosted command writes output to stdout or stderr, and contains the output produced by the command.
`PM_DISCOVER` | [discovery_t](\ref procman_lcm_discovery_t) | Published to check for conflicting deputies with the same ID.  Published when a deputy first starts up.

## Messages received by deputy {#procman_comms_deputy_received}

The deputy subscribes to the following messages:

LCM Channel | Message type | Description
------------|--------------|-------------
`PM_DISOCVER` | [discovery_t](\ref procman_lcm_discovery_t) | When received, the deputy replies by transmitting its state on channel `PM_INFO`.
`PM_ORDERS` | [orders_t](\ref procman_lcm_orders_t) | When received, the deputy adds/removes/starts/stops commands as indicated by the orders. There are some basic checks to ensure that orders are actually targeted for the deputy.

# Sheriff {#procman_comms_sheriff}

## Messages transmitted by sheriff {#procman_comms_sheriff_transmitted}

LCM Channel | Message type | Description
------------|--------------|------------
`PM_ORDERS` | [orders_t](\ref procman_lcm_orders_t) | Commands a deputy.  Each orders message contains the desired state of all commands managed by a single deputy.  To command multiple deputies, the sheriff sends one orders message to each deputy.  Transmitted at 1 Hz or when a command's desired status changes.
`PM_DISCOVER` | [discovery_t](\ref procman_lcm_discovery_t) | Published by a sheriff to discover deputies when the sheriff first starts up.

## Messages received by sheriff {#procman_comms_sheriff_received}

LCM Channel | Message type | Description
------------|--------------|------------
`PM_INFO` | [deputy_info_t](\ref procman_lcm_deputy_info_t) | When received, the sheriff updates its internal representation of a deputy's actual state.
`PM_OUTPUT` | [output_t](\ref procman_lcm_output_t) | Contains the console output produced by a deputy-managed command.  When received, the sheriff may display a command's output to the user.  Subscribing to this channel is optional for a sheriff.


# Appendix - message definitions {#procman_lcm_message_defs}

## procman_lcm.deputy_info_t {#procman_lcm_deputy_info_t}
\include procman_lcm_deputy_info_t.lcm

## procman_lcm.cmd_status_t {#procman_lcm_cmd_status_t}
\include procman_lcm_cmd_status_t.lcm

## procman_lcm.cmd_t {#procman_lcm_cmd_t}
\include procman_lcm_cmd_t.lcm

## procman_lcm.output_t {#procman_lcm_output_t}
\include procman_lcm_output_t.lcm

## procman_lcm.discovery_t {#procman_lcm_discovery_t}
\include procman_lcm_discovery_t.lcm

## procman_lcm.orders_t {#procman_lcm_orders_t}
\include procman_lcm_orders_t.lcm
