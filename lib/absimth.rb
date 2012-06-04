require 'actor'

require 'rubygems'
require 'ffi-rxs'
require 'uuid'

# I don't really want to do this monkey patching... but I kinda do
class Hash
  def to_uuid
    if self.include? :to_uuid
      return self[:to_uuid]
    end
  end
end

module Absimth

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

      define_method name do
        instance_variable_get("@#{name}")
      end

      define_method (name + '=') do |v|
        instance_variable_set("@#{name}", v)
      end

      default = opts[:default]
      unless default.nil?
        unless self.class_variable_defined?(:@@init_hooks)
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

        sleep rand(2.0) # temporary measure :P

        messages = true
        while messages
          Actor.receive do |f|
            f.when(Hash) do |msg|
              if msg[:type] == :interaction
                self.from = AgentWrapper.new(*msg[:from])
                self.send(msg[:method], *msg[:args])
              end
              if msg[:signal] == :done
                running = false
                messages = false
              end
            end
            f.when(ANY) do
              puts "#{self.class}(#{self.object_id}): Don't know how to handle this message: #{msg}"
            end
            f.after(0.0) do
              messages = false
            end
          end
        end # done checking messages

        @timestamp += 1
        act

      end # done running
    end

  end # module Agent

  class AgentDelegate

    def uuid
      @uuid
    end

    def cls
      @cls
    end

    def send(msg)
      raise "bare AgentWrapper getting a message!"
    end
    alias_method :<<, :send

  end # class AgentDelegate

  class LocalAgentDelegate < AgentDelegate
    def initialize(cls, opts)
      @cls = cls
      @uuid = opts.delete(:agent_uuid)
      @actor = Actor.spawn_link do
        begin
          Thread.current[:agent_delegate] = self
          cls.new(opts).loop
        rescue Exception => e
          logfile = File.join("logs", "#{Actor.current.object_id}.crash")
          File.open(logfile, "a") { |f|
            e.render("#{@cls}(#{Actor.current.object_id}) crashed", f, false)
          }
          puts "#{@cls}(#{Actor.current.object_id}) crashed"
        end
      end
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
      Actor[:comms] << msg
    end
    alias_method :<<, :send

  end # class RemoteAgentDelegate

  class AgentWrapper

    def initialize(cls, uuid)
      @cls = cls
      @uuid = uuid
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
      delegate = Thread.current[:agent_delegate]
      if @cls.accepts_interaction?(meth)
        self << {
          :type => :interaction,
          :method => meth,
          :args => args,
          :from => [delegate.cls, delegate.uuid]
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
      @control_endpoint = opts[:control_endpoint]
      @comm_endpoint = opts[:comm_endpoint]
    end

  end # class Simulation

  class SimulationMaster < Simulation

    def initialize(opts={}, &blk)
      raise "No block given to simulation master" unless blk
      @spawn_blk = blk
      @known_slaves = Set[]
      super
    end

    def run(t=0)
      @ctx = XS::Context.create
      @control_faucet = ControlFaucet.new(@ctx, :endpoint => @control_endpoint)
      # I really, REALLY shouldn't have to do this here, but somebody has to bind
      @comms = @ctx.socket(XS::PUB)
      @comms.setsockopt(XS::LINGER, 0)
      @comms.bind(@comm_endpoint)

      puts "Alright, gonna do some spawning and stuff"

      self.instance_eval &@spawn_blk
      puts "Alright, we are done setting up, let's take a nap"
      sleep t

      while @known_slaves.size > 0 do
        @known_slaves -= Set[@control_faucet.send(:signal => :done)]
      end

      @control_faucet.close
      @comms.close
      @ctx.terminate

      puts "Cleaned up fine"
    end

    def spawn(cls, opts={})
      agent_uuid = UUID.generate
      slave_addr = @control_faucet.send({
        :signal => :spawn,
        :cls => cls,
        :agent_uuid => agent_uuid,
      }.merge(opts))
      @known_slaves += Set[slave_addr]
      return AgentWrapper.new(cls, agent_uuid)
    end

  end # class SimulationMaster

  class SimulationSlave < Simulation

    def initialize(opts={})
      @agent_delegates = {}
      Thread.current[:agent_delegates] = @agent_delegates
      super
    end

    def run(t=0)
      @ctx = XS::Context.create
      @control_sink = ControlSink.new(@ctx, :endpoint => @control_endpoint)
      @comms = CommHub.new(@ctx, :endpoint => @comm_endpoint)

      comm_recv_actor = Actor.new do
        puts "Alright, listening for incoming comms"
        loop do
          comm_msg = @comms.recv
          handle_comms_msg comm_msg
          print ">-<"
          Actor.receive do |f|
            f.when(Hash) do |msg|
              if msg[:signal] == :done
                break
              end
            end
            f.after(0.0) { nil }
          end
        end
      end
      comm_send_actor = Actor.new do
        puts "Alright, listening for outgoing comms"
        loop do
          msg = Actor.receive
          if msg.is_a?(Hash) and msg[:signal] == :done
            msg[:from] << {:signal => :ok}
            break
          end
          if msg.respond_to? :to_uuid
            print "<->"
            @comms.send(msg)
          end
        end
      end
      Actor.register(:comms, comm_send_actor)

      puts "Alright, listening for control signals"
      loop do
        control_msg = @control_sink.recv
        if handle_control_msg(control_msg) == :done
          break
        else
          @control_sink.ready
        end
      end

      puts "\nOkay, guess we're done"

      @agent_delegates.each do |uuid,delegate|
        delegate << {:signal => :done}
      end

      comm_send_actor << {:signal => :done}
      comm_recv_actor << {:signal => :done}

      @control_sink.close
      @comms.close
      @ctx.terminate

      puts "Cleaned up safely"
    end

    def handle_control_msg(msg)
      if msg[:signal] == :spawn
        a = spawn msg.delete(:cls), msg
      end
      return msg[:signal]
    end

    def handle_comms_msg(msg)
      puts "Whoa, got a comms message"
      @agent_delegates[msg.to_uuid] << msg
    end

    def spawn(cls, opts={})
      unless @comms
        raise "SimulationSlave#spawn called without a connected comms pipe!"
      end
      # TODO: Should do the spawning wherever is most appropriate
      # For simplicity, we'll initially just rely on push/pull sockets
      aw = LocalAgentDelegate.new(cls, opts)
      @agent_delegates[aw.uuid] = aw
      @comms.subscribe(aw.uuid)
      return aw
    end

  end # class SimulationSlave

  class Pipe

    def recv_str socket
      str = ''
      ec socket.recv_string(str)
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

      slave_addr = recv_str @socket
      empty = recv_str @socket
      slave_msg = recv_str @socket
      raise "Malformed message from slave" unless load_obj(slave_msg)[:signal] == :ready

      ec @socket.send_string(slave_addr, XS::SNDMORE)
      ec @socket.send_string('', XS::SNDMORE)
      ec @socket.send_string(str)
      return slave_addr
    end

    def close
      @socket.close
    end

  end # class ControlFaucet

  class ControlSink < Pipe

    def initialize(ctx, opts={})
      raise "Need options for pipe" unless !opts.empty?
      @socket = ctx.socket(XS::REQ)
      @socket.setsockopt(XS::LINGER, 0)
      @socket.setsockopt(XS::IDENTITY, UUID.generate)
      @socket.connect(opts[:endpoint])

      send(:signal => :ready)
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
      send(:signal => :ready)
    end

    def close
      @socket.close
    end

  end # class ControlSink

  class CommHub < Pipe

    def initialize(ctx, opts={})
      @out_socket = ctx.socket(XS::PUB)
      @out_socket.setsockopt(XS::LINGER, 0)
      @out_socket.connect(opts[:endpoint])
      @in_socket = ctx.socket(XS::SUB)
      @in_socket.setsockopt(XS::LINGER, 0)
      @in_socket.connect(opts[:endpoint])
    end

    def send(obj)
      unless obj.respond_to? :to_uuid
        raise "Can't send a to_uuid-less object on comms!"
      end
      ec @out_socket.send_string(obj.to_uuid, XS::SNDMORE)
      msg = dump_obj obj
      ec @out_socket.send_string(msg)
    end

    def recv
      # Truncate the initial to_uuid
      puts "YERP DERP GONNA TRY TO RECV"
      to_uuid = recv_str(@in_socket)
      puts "WHOOOAAA, got a UUID"
      str = recv_str(@in_socket)
      puts "OKAY, EVEN GOT A MSG GONNA MARSHAL IT NOW LOL"
      return load_obj(str)
    end

    def subscribe(str)
      ec @in_socket.setsockopt(XS::SUBSCRIBE, str)
    end

    def close
      ec @out_socket.close
      ec @in_socket.close
    end

  end # class CommHub

end # module Absimth

