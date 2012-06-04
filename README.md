# Absimth: A Distributed Agent-based Modeling Framework for Ruby

Distributed, parallel computations are hard. Global state, virtual time, and numerous other considerations have made distributed computing a tough realm to break into for those wishing to do agent-based modeling.

Aiming to make this easier, Absimth will allow simple, descriptive declarations of agents with plenty of flexibility for how agents interact.

## Planned features

* **Rollback** of agent state to keep the simulation correct even when some nodes or agents are running slower or faster than others.
* **Message routing** to use the quickest delivery mechanism, whether another agent is on the same machine or across the globe.
* **Census/reporting** for flexible data collection throughout a simulation.

## Hopeful future features

* **Environment**, which is crucial for most useful models, must be implemented as an agent or set of agents, if only the minimal features above are present. This is often not the most efficient.
* **Automated agent localization** to move agents most likely to communicate with each other onto the same node.
* **Easy experiment scheduling** of multiple runs for stochastic models.

## Usage

**N.B.: Absimth requires [Rubinius](http://rubini.us/)** and always will. I only test on `rbx-2.0.0-dev`. Rubinius's built in Channels, stdlib Actors, and actually good threads are crucial to Absimth.

Running a sim should be as easy as:

    $ rbx test_sim.rb

And for now, `test_sim.rb` is all we've got in the way of documentation or examples. Sorry.

