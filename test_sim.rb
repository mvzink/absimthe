# Test/example simulation(s)
# Design first: the API is the UI is the spec.
# Start at the top: ignore the details until they are necessary.

require './lib/absimthe'

require 'set'

def assert(msg=nil)
  raise (msg || "Assertion failed") unless yield
end

class Patron
  extend Absimthe::Agent
  attribute :paintings, :default => 0
  attribute :painting_lust, :default => 0.0
  attribute :location
  attribute :known_galleries, :default => proc { Set.new }
  attribute :bureau
  attribute :checked_in, :default => false

  interactions [
    :ask_about_galleries,
    :tell_about_gallery,
    :send_painting,
    :tell_no_painting_available,
    :see_occupants
  ]

  def act
    unless self.checked_in
      self.bureau.patron_checkin
      self.checked_in = true
    end
    if self.painting_lust >= 10.0
      # try to get a painting
      if not self.location.nil?
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
    self.bureau.patron_report(:galleries => self.known_galleries.size, :paintings => self.paintings)
    sleep rand # temporary measure :P
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
    # print "{#{Actor.current.object_id}:+}" unless self.known_galleries.include?(g)
    self.known_galleries += Set[g]
    # TODO: Shouldn't be making this check once clocks/rollbacks exist
    self.location.leave if self.location
    self.location = nil
  end

end

class Gallery
  extend Absimthe::Agent

  attribute :occupants, :default => proc { Set.new }
  attribute :paintings, :default => 1
  attribute :painting_production, :default => 1
  attribute :bureau
  attribute :checked_in, :default => false

  interactions [
    :request_painting,
    :enter,
    :leave,
    :look_around
  ]

  def act
    unless self.checked_in
      self.bureau.gallery_checkin
      self.checked_in = true
    end
    if rand(2)
      self.paintings += self.painting_production
      # print "[#{Actor.current.object_id}:+:#{self.paintings}]"
    end
    sleep rand # temporary measure :P
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

class CensusBureau
  extend Absimthe::Agent

  attribute :patrons, :default => proc { Hash.new }
  attribute :known_patrons, :default => 0
  attribute :known_galleries, :default => 0

  interactions [
    :gallery_checkin,
    :patron_checkin,
    :patron_report
  ]

  def gallery_checkin
    self.known_galleries += 1
  end

  def patron_checkin
    self.known_patrons += 1
  end

  def patron_report(data)
    self.patrons[from.uuid] = data
  end

  def act
    actual_ng = self.patrons.values.reduce(0) { |m,o| m + o[:galleries] }
    actual_np = self.patrons.values.reduce(0) { |m,o| m + o[:paintings] }
    desired_ng = self.known_patrons * self.known_galleries
    if self.patrons.keys.empty?
      avg_ng = 0
      avg_np = 0
    else
      avg_ng = actual_ng.to_f / self.known_patrons
      avg_np = actual_np.to_f / self.known_patrons
    end
    puts "#{@timestamp}: Currently #{actual_ng} of #{desired_ng}. Of #{self.known_patrons} patrons, the average one knows #{avg_ng} of #{self.known_galleries} galleries and has #{avg_np} paintings."
    sleep 0.2
  end

end

def test_simple_sim(opts={})
  t = opts.delete(:time)
  if opts.delete(:master)
    sim = Absimthe::SimulationMaster.new opts do
      bureau = spawn CensusBureau
      last_gallery = nil
      11.times do
        this_gallery = spawn Gallery, :bureau => bureau,
          :painting_production => params[:painting_production]
        11.times do
          spawn Patron, :known_galleries => Set[this_gallery], :bureau => bureau
          if last_gallery
            spawn Patron, :known_galleries => Set[last_gallery, this_gallery], :bureau => bureau
          end
        end
        last_gallery = this_gallery
      end
      11.times do
        spawn Patron, :known_galleries => Set[last_gallery], :bureau => bureau
      end
    end
  else
    sim = Absimthe::SimulationSlave.new opts
  end
  sim.run(t, :painting_production => 2) # TODO: Exit condition lol; probably clock limit on sim
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

