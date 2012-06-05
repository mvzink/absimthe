require 'actor'

require 'rubygems'
require 'ffi-rxs'
require 'uuid'
require 'set'

# I don't really want to do this monkey patching... but I kinda do
class Hash
  def to_uuid
    if self.include? :to_uuid
      return self[:to_uuid]
    end
  end
end

# Wait no, monkey patching is the best
class Actor
  def kill
    @thread.kill
  end
end

module Absimthe

  ANY = Object.new
  def ANY.===(obj)
    true
  end

  module Agent

    def self.extended(cls)
      cls.send :include, self
      super
    end

    def initialize(opts={})
      @timestamp = 0
      self.class.class_variable_get(:@@init_hooks).each do |name,default_blk|
        self.instance_eval(&default_blk) unless opts.include?(name.to_sym)
      end
      opts.each do |k,v|
        m = "#{k}="
        if respond_to? m
          send(m, v)
        end
      end
    end

    def attribute(name, opts={})
      name = name.to_s if name.kind_of?(Symbol)

      unless self.class_variable_defined?(:@@attributes)
        self.class_variable_set(:@@attributes, [])
      end
      attrs = self.class_variable_get(:@@attributes)
      attrs << name

      define_method name do
        if not instance_variable_defined?("@#{name}")
          instance_variable_set("@#{name}", {})
          nil
        else
          hist = instance_variable_get("@#{name}")
          if hist.has_key? @timestamp
            hist[@timestamp]
          elsif hist.keys.empty?
            nil
          else
            max = hist.keys.max
            hist[max]
          end
        end
      end

      define_method (name + '=') do |v|
        if not instance_variable_defined?("@#{name}")
          instance_variable_set("@#{name}", {})
        end
        hist = instance_variable_get("@#{name}")
        hist[@timestamp] = v
      end

      define_method ('rollback_' + name) do
        if not instance_variable_defined?("@#{name}")
          instance_variable_set("@#{name}", {})
        else
          vars = instance_variable_get("@#{name}")
          vars.delete_if do |k,v|
            @timestamp < k
          end
        end
      end

      default = opts[:default]
      if not default.nil?
        if not self.class_variable_defined?(:@@init_hooks)
          self.class_variable_set(:@@init_hooks, {})
        end
        ihs = self.class_variable_get(:@@init_hooks)
        ihs[name] = proc {
          if default.kind_of?(Proc)
            v = default.call
          else
            v = default
          end
          send(name + '=', v)
        }
      end
    end

    def timestamp
      @timestamp
    end

    def timestamp= t
      @timestamp = t
      Thread.current[:agent_delegate].timestamp = t
    end

    def rollback(t)
      @timestamp = t
      if class_variable_defined?(:@@attributes)
        attrs = class_variable_get(:@@attributes)
        attrs.each do |attr|
          self.send('rollback_' + attr)
        end
      end
    end

    def interactions(ary=nil)
      unless self.class_variable_defined?(:@@interactions)
        self.class_variable_set(:@@interactions, Set[])
      end
      list = self.class_variable_get(:@@interactions)
      if ary
        list = list | ary
        self.class_variable_set(:@@interactions, list)
      end
      return list
    end

    def accepts_interaction?(sym)
      return self.interactions.include?(sym)
    end

    def from
      return self.instance_variable_get(:@from)
    end

    def from=(o)
      self.instance_variable_set(:@from, o)
    end

    def loop
      # TODO: Exit condition lol; probably clock limit on sim
      running = true
      while running

        messages = true
        while messages
          begin
            Actor.receive do |f|
              f.when(Hash) do |msg|
                if msg[:type] == :interaction
                  if msg[:timestamp] < @timestamp
                    rollback msg[:timestamp]
                  end
                  self.from = AgentWrapper.new(*msg[:from])
                  self.send(msg[:method], *msg[:args])
                end
              end
              f.when(ANY) do |msg|
                puts "#{self.class}(#{self.object_id}): Don't know how to handle this message: #{msg}"
              end
              f.after(0.0) do
                messages = false
              end
            end
          rescue NoMethodError
            # TODO: Fix this; possible bug with Rubinius' Actor?
            # We get seemingly random NoMethodErrors w/ indecipherable relation to user code.
          end
        end # done checking messages

        self.timestamp += 1
        act

      end # done running
      print "¡#{self.object_id}!"
    end

  end # module Agent

  class AgentDelegate

    def uuid
      @uuid
    end

    def cls
      @cls
    end

    def kill
      false
    end

    def send(msg)
      raise "bare AgentDelegate getting a message!"
    end
    alias_method :<<, :send

  end # class AgentDelegate

  class LocalAgentDelegate < AgentDelegate
    def initialize(cls, opts)
      @cls = cls
      @timestamp = 0
      @uuid = opts.delete(:agent_uuid)
      @actor = Actor.spawn_link do
        begin
          Thread.current[:agent_delegate] = self
          cls.new(opts).loop
        rescue => e
          unless e.is_a? Thread::Die
            logfile = File.join("logs", "#{Actor.current.object_id}.crash")
            File.open(logfile, "a") { |f|
              e.render("#{@cls}(#{Actor.current.object_id}) crashed", f, false)
            }
            puts "#{@cls}(#{Actor.current.object_id}) crashed"
          end
        end
      end
    end

    def timestamp
      @timestamp
    end

    def timestamp= t
      @timestamp = t
    end

    def kill
      @actor.kill
    end

    def send(msg)
      @actor << msg
    end
    alias_method :<<, :send

  end # class LocalAgentDelegate

  class RemoteAgentDelegate < AgentDelegate

    def initialize(cls, uuid)
      @cls = cls
      @uuid = uuid
    end

    def send(msg)
      msg[:to_uuid] = @uuid
      comm_horn_actor = Actor[:comm_horn]
      unless comm_horn_actor.nil?
        Actor[:comm_horn] << msg
      end
    end
    alias_method :<<, :send

    def kill
      true
    end

  end # class RemoteAgentDelegate

  class AgentWrapper

    def initialize(cls, uuid)
      @cls = cls
      @uuid = uuid
    end

    def uuid
      @uuid
    end

    def update_delegate
      @delegate = nil
      thr = Thread.list.select { |t| !t[:agent_delegates].nil? }.first
      if thr
        @delegate = thr[:agent_delegates][@uuid]
      end
      unless @delegate
        @delegate = RemoteAgentDelegate.new(@cls, @uuid)
      end
      @delegate
    end

    def method_missing(meth, *args, &blk)
      sender = Thread.current[:agent_delegate]
      if @cls.accepts_interaction?(meth)
        self << {
          :type => :interaction,
          :method => meth,
          :args => args,
          :from => [sender.cls, sender.uuid],
          :timestamp => sender.timestamp
        }
      else
        super
      end
    end

    def send(msg)
      (@delegate or update_delegate) << msg
    end
    alias_method :<<, :send

  end # class AgentWrapper

  class Simulation
    def initialize(opts={})
      # TODO: Validate options with XS
      @control_endpoint = opts.delete(:control_endpoint)
      @comm_endpoint = opts.delete(:comm_endpoint)
    end

  end # class Simulation

  class SimulationMaster < Simulation

    def initialize(opts={}, &blk)
      raise "No block given to simulation master" unless blk
      @spawn_blk = blk
      super
    end

    def params
     @params
    end

    def run(t=0, params={})
      @params = params
      @ctx = XS::Context.create
      @control_faucet = ControlFaucet.new(@ctx, :endpoint => @control_endpoint)

      puts "Gonna collect slave info"
      slaves = {}
      # Holy god this is janky
      misses = 0
      while misses < 1000 do
        rep = @control_faucet.send(:signal => :ping)
        if slaves.has_key? rep[:slave_uuid]
          misses += 1
        else
          slaves[rep[:slave_uuid]] = rep[:endpoint]
        end
      end
      puts "Pushing out #{slaves.size} endpoints for comms listening: #{slaves.inspect}"
      @control_faucet.send_all(slaves.keys, :signal => :listen, :endpoints => slaves.values)

      puts "Alright, gonna do some spawning and stuff"

      self.instance_eval &@spawn_blk
      puts "Alright, we are done setting up, let's take a nap"
      sleep t

      @control_faucet.send_all(slaves.keys, :signal => :done)

      @control_faucet.close
      @ctx.terminate

      puts "Cleaned up fine"
    end

    def spawn(cls, opts={})
      agent_uuid = UUID.generate
      rep = @control_faucet.send({
        :signal => :spawn,
        :cls => cls,
        :agent_uuid => agent_uuid,
      }.merge(opts))
      return AgentWrapper.new(cls, agent_uuid)
    end

  end # class SimulationMaster

  class SimulationSlave < Simulation

    def initialize(opts={})
      @agent_delegates = {}
      Thread.current[:agent_delegates] = @agent_delegates
      super
    end

    def run(t=0, params={})
      @ctx = XS::Context.create
      @control_sink = ControlSink.new(@ctx, :endpoint => @control_endpoint, :comm_endpoint => @comm_endpoint)
      @comm_ear = CommEar.new(@ctx, :comm_endpoint => @comm_endpoint)
      @comm_ear_listen_actor = nil
      @comm_ear_control_actor = Actor.new do
        msg = Actor.receive
        case msg[:signal]
        when :done
          break
        when :listen
          msg[:endpoints].each do |endpt|
            @comm_ear.listen(endpt)
          end
          @comm_ear_listen_actor = Actor.new do
            puts "Alright, listening for incoming comms"
            loop do
              comm_msg = @comm_ear.recv
              handle_comm_msg comm_msg
            end
            puts "Okay, done listening for incoming comms"
          end
        when :subscribe
          @comm_ear.subscribe(msg[:uuid])
          puts "I ACTUALLY SUBSCRIBED TO SOMETHING"
        end
      end
      @comm_horn = CommHorn.new(@ctx, :endpoint => @comm_endpoint)
      @comm_horn_actor = Actor.new do
        puts "Alright, listening for outgoing comms"
        loop do
          msg = Actor.receive
          if msg.is_a?(Hash) and msg[:signal] == :done
            @comm_horn.close
            break
          end
          if msg.respond_to? :to_uuid
            @comm_horn.send(msg)
          end
        end
        puts "Okay, done listening for outgoing comms"
      end
      Actor.register(:comm_horn, @comm_horn_actor)

      puts "Alright, listening for control signals"
      loop do
        control_msg = @control_sink.recv
        if handle_control_msg(control_msg) == :done
          break
        else
          @control_sink.ready
        end
      end

      puts "\nOkay, guess we're done. We ran #{@agent_delegates.size} agents locally."

      @agent_delegates.each do |uuid, delegate|
        delegate.kill
      end

      @comm_horn_actor.kill
      @comm_ear_control_actor.kill
      @comm_ear_listen_actor.kill

      @control_sink.close
      @comm_horn.close
      @comm_ear.close
      @ctx.terminate

      puts "Cleaned up safely"
    end

    def handle_control_msg(msg)
      if msg[:signal] == :spawn
        a = spawn msg.delete(:cls), msg
      elsif msg[:signal] == :listen
        @comm_ear_control_actor << msg
      end
      return msg[:signal]
    end

    def handle_comm_msg(msg)
      a = @agent_delegates[msg.to_uuid]
      a << msg unless a.nil?
    end

    def spawn(cls, opts={})
      unless @comm_horn_actor
        raise "SimulationSlave#spawn called without connected comm pipes!"
      end
      # TODO: Should do the spawning wherever is most appropriate
      # For simplicity, we'll initially just rely on push/pull sockets
      aw = LocalAgentDelegate.new(cls, opts)
      @agent_delegates[aw.uuid] = aw
      @comm_ear_control_actor << {:signal => :subscribe, :uuid => aw.uuid}
      return aw
    end

  end # class SimulationSlave

  class Pipe

    def recv_str socket, opts={}
      str = ''
      ec socket.recv_string str
      str
    end

    def load_obj(str)
      return Marshal.load(str, lambda {|obj|
        if obj.class == AgentWrapper
          obj.update_delegate
        end
      })
    end

    def dump_obj(obj)
      Marshal.dump(obj)
    end

    def ec(rc)
      if XS::Util.resultcode_ok?(rc)
        true
      else
        raise "XS operation failed [#{XS::Util.errno}]: #{XS::Util.error_string}"
      end
    end

  end # class Pipe

  class ControlFaucet < Pipe

    def initialize(ctx, opts={})
      @socket = ctx.socket(XS::ROUTER)
      @socket.bind(opts[:endpoint])
    end

    def send(obj)
      str = dump_obj obj

      slave_uuid = recv_str @socket
      empty = recv_str @socket
      slave_str = recv_str @socket
      slave_msg = load_obj(slave_str)
      raise "Malformed message from slave" unless slave_msg[:slave_uuid] == slave_uuid

      ec @socket.send_string(slave_uuid, XS::SNDMORE)
      ec @socket.send_string('', XS::SNDMORE)
      ec @socket.send_string(str)
      return slave_msg
    end

    def send_all(uuids, obj)
      uuids = uuids.to_set
      while uuids.size > 0 do
        rep = send(obj)
        uuids -= Set[rep[:slave_uuid]]
      end
    end

    def close
      @socket.close
    end

  end # class ControlFaucet

  class ControlSink < Pipe

    def initialize(ctx, opts={})
      raise "Need options for pipe" unless !opts.empty?
      @comm_endpoint = opts[:comm_endpoint]
      @uuid = UUID.generate
      @socket = ctx.socket(XS::REQ)
      @socket.setsockopt(XS::LINGER, 0)
      @socket.setsockopt(XS::IDENTITY, @uuid)
      @socket.connect(opts[:endpoint])

      ready
    end

    def send obj
      str = dump_obj obj
      rc = ec @socket.send_string(str)
      rc
    end

    def recv
      load_obj recv_str(@socket)
    end

    def ready
      send(:signal => :ready, :endpoint => @comm_endpoint, :slave_uuid => @uuid)
    end

    def close
      @socket.close
    end

  end # class ControlSink

  class CommHorn < Pipe

    def initialize(ctx, opts={})
      @socket = ctx.socket(XS::PUB)
      @socket.setsockopt(XS::LINGER, 0)
      @socket.bind(opts[:endpoint])
   end

    def send(obj)
      unless obj.respond_to? :to_uuid
        raise "Can't send a to_uuid-less object on comms!"
      end
      msg = dump_obj obj
      ec @socket.send_string(obj.to_uuid + msg)
    end

    def close
      ec @socket.close
    end

  end # class CommHorn

  class CommEar < Pipe

    def initialize(ctx, opts={})
      @socket = ctx.socket(XS::SUB)
      @socket.setsockopt(XS::LINGER, 0)
      @comm_endpoint = opts[:comm_endpoint]
      # Fuck it, subscribe all the things
      # TODO: Fix the actual subscribe filters
      @socket.setsockopt(XS::SUBSCRIBE, '')
    end # class CommEar

    def recv
      # Truncate the initial to_uuid
      str = recv_str(@socket)
      return load_obj(str[36..-1])
    end

    def listen(endpt)
      puts "#{@comm_endpoint}  =? #{endpt}"
      if @comm_endpoint == endpt
        puts "FFFFFFUUUUUUU"
      else
        ec @socket.connect(endpt)
        puts "NO WAY, I AM LISTENING TO #{endpt}"
      end
    end

    def subscribe(str)
      rc = ec @socket.setsockopt(XS::SUBSCRIBE, str)
      rc
    end

    def close
      ec @socket.close
    end

  end # class CommEar

end # module Absimthe

