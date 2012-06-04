require 'actor'

def log(n, m)
  puts "#{n}: #{m}"
end

def ring(n)
  Actor.spawn do
    target = nil
    ready = false
    dying = false

    log n, "starting"

    until dying do
      log n, "waiting for msg"
      msg = Actor.receive do |f|
        f.when(Actor) { |who|
          log n, "setting target (#{target} => #{who})"
          target = who
          target << :ready
        }
        f.when(:ready) {
          log n, "ready"
          ready = true
        }
        f.when(:dying) {
          if ready
            log n, "dying correctly"
            dying = true
            target << :dying
          else
            log n, "got premature death"
          end
        }
      end
    end

    log n, "correctly died"
  end
end

last = ring(0)

5.times do |i|
  t = ring(i)
  last << t
  last = t
end

last << :ready
last << :dying

sleep 2

