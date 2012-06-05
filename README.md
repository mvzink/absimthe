# Absimthe: A Distributed Agent-based Modeling Framework for Ruby

*Current status: prototype*

Distributed, parallel computations are hard. Global state, virtual time, and numerous other considerations have made distributed computing a tough realm to break into for those wishing to do agent-based modeling.

Aiming to make this easier, Absimthe will allow simple, descriptive declarations of agents with plenty of flexibility for how agents interact.

## Current features

The crucial architectural features are largely implemented (but with a big "prototype" label)

* **Rollback** of agent state to keep the simulation correct even when some nodes or agents are running slower or faster than others.
* **Automatic message routing** to get messages from agent to agent regardless of which node each is located on.

Things which I thought would be features but which can actually be implemented pretty easily as agents:

* **Census/reporting** for flexible data collection throughout a simulation.

## Planned features

Aside from bugfixes and performance improvements (of which there are many to be made), here are some of the major changes currently on the drawing table.

* **Automated agent localization** to move agents most likely to communicate with each other onto the same node.
* **Manual agent placement** for fine-grained control over special agents (e.g. reporting/census agents)
* **Easy experiment scheduling** of multiple runs (e.g. for stochastic models with sensitive initial conditions, or for varying certain parameters) 
* **Environment**, which is crucial for most useful models, must currently be implemented as an agent or set of agents. This is often not the most efficient, so some level of global or node-local state may be desirable.

## Usage

**N.B.: Absimthe requires [Rubinius](http://rubini.us/)** and always will. I only test on `rbx-2.0.0-dev`. Rubinius's built in Channels, stdlib Actors, and actually good threads are crucial to Absimthe.

Running a sim should be as easy as:

    $ rbx test_sim.rb

And for now, `test_sim.rb` is all we've got in the way of documentation or examples. Sorry.

## A note on naming conventions

Leave me alone.

