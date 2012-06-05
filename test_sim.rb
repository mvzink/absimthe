# Test/example simulation(s)
# Design first: the API is the UI is the spec.
# Start at the top: ignore the details until they are necessary.

require './lib/absimth'

require 'set'

def assert(msg=nil)
  raise (msg || "Assertion failed") unless yield
end

class Patron
  extend Absimth::Agent
  attribute :paintings, :default => 0
  attribute :painting_lust, :default => 0.0
  attribute :location
  attribute :known_galleries, :default => proc { Set.new }

  interactions [
    :ask_about_galleries,
    :tell_about_gallery,
    :send_painting,
    :tell_no_painting_available,
    :see_occupants
  ]

  def act
    sleep rand # temporary measure :P
    if self.painting_lust >= 10.0
      # try to get a painting
      if self.location
        self.location.request_painting
      elsif !self.known_galleries.empty?
        g = self.known_galleries.to_a[rand(self.known_galleries.size)]
        self.location = g
        self.location.enter
        # print "{#{Actor.current.object_id}:e}"
        self.painting_lust += rand * 5.0
      end
    else
      self.painting_lust += rand * 5.0
    end
  end

  def send_painting
    self.paintings += 1
    self.painting_lust -= 10.0
    # TODO: This should be replaced with census/reporting
    # print "{#{Actor.current.object_id}:!}"
    # TODO: Shouldn't be making this check once clocks/rollbacks exist
    if self.location
      self.location.leave
      self.location = nil
    end
  end

  def tell_no_painting_available
    # TODO: Shouldn't be making this check once clocks/rollbacks exist
    if self.location
      self.location.look_around
      # print "{#{Actor.current.object_id}:?}"
    end
  end

  def see_occupants(occupants)
    o = occupants.to_a[rand(occupants.size)]
    o.ask_about_galleries
    # print "{#{Actor.current.object_id}:o}"
  end

  def ask_about_galleries
    unless self.known_galleries.empty?
      # print "{#{Actor.current.object_id}:>}"
      g = self.known_galleries.to_a[rand(self.known_galleries.size)]
      from.tell_about_gallery(g)
    end
  end

  def tell_about_gallery(g)
    # TODO: This should be replaced with census/reporting
    print "{#{Actor.current.object_id}:+}" unless self.known_galleries.include?(g)
    self.known_galleries += Set[g]
    # TODO: Shouldn't be making this check once clocks/rollbacks exist
    self.location.leave if self.location
    self.location = nil
  end

end

class Gallery
  extend Absimth::Agent

  attribute :occupants, :default => proc { Set.new }
  attribute :paintings, :default => 1

  interactions [
    :request_painting,
    :enter,
    :leave,
    :look_around
  ]

  def act
    sleep rand # temporary measure :P
    if rand(2)
      self.paintings += 1
      # print "[#{Actor.current.object_id}:+:#{self.paintings}]"
    end
  end

  def request_painting
    if self.paintings > 0
      self.paintings -= 1
      # print "[#{Actor.current.object_id}:-:#{self.paintings}]"
      from.send_painting
    else
      # print "[#{Actor.current.object_id}:_]"
      from.tell_no_painting_available
    end
  end

  def enter
    self.occupants += Set[from]
  end

  def leave
    self.occupants -= Set[from]
  end

  def look_around
    from.see_occupants(self.occupants)
  end

end

def test_simple_sim(opts={})
  t = opts.delete(:time)
  if opts.delete(:master)
    sim = Absimth::SimulationMaster.new opts do
      last_gallery = nil
      9.times do
        this_gallery = spawn Gallery
        9.times do
          spawn Patron, :known_galleries => Set[this_gallery]
          if last_gallery
            spawn Patron, :known_galleries => Set[last_gallery, this_gallery]
          end
        end
        last_gallery = this_gallery
      end
      9.times do
        spawn Patron, :known_galleries => Set[last_gallery]
      end
    end
  else
    sim = Absimth::SimulationSlave.new opts
  end
  sim.run(t) # TODO: Exit condition lol; probably clock limit on sim
end

if __FILE__ == $0
  require 'rubygems'
  require 'trollop'
  opts = Trollop::options do
    # TODO: Consider implementing automatic leader election
    opt :master, "Designate this process to lead the simulation", :default => false
    opt :control_endpoint,
      "ZMQ-compatible endpoint for slave-master simulation control/coordination",
      :default => "ipc://.ipc/absimth_control"
    opt :comm_endpoint,
      "ZMQ-compatible endpoint for inter-node agent communication (each slave needs a unique endpoint)",
      :default => "ipc://.ipc/absimth_comms"
    opt :time,
      "Sleep the process for this many seconds",
      :default => 3
  end

  if opts[:master]
    Dir.glob(File.join("logs", "*.crash")).each do |f|
      File.delete(f)
    end
  end

  test_simple_sim opts
  puts "Everything went better than expected."
end

