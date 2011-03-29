module Resque
  module Plugins
    module PriorityQueue

      def self.enable!(options={})
        return if @priority_queue_enabled

        compat = options[:compat] || []
        compat = [compat] unless compat.is_a?(Array)

        Resque.send(:include, ResqueMethods)

        Resque.class_eval do
          push_method = compat.include?(:lifecycle) ? :push_without_lifecycle : :push

          alias_method :push_sequential, push_method
          alias_method push_method, :_push

          alias_method :pop_sequential, :pop
          alias_method :pop, :_pop

          alias_method :size_sequential, :size
          alias_method :size, :_size

          extend self
        end

        Resque::Job.send(:extend, JobMethods)
        Resque::Job.send(:include, JobMethods)

        @priority_queue_enabled = true
      end

      module ResqueMethods

        def is_priority_queue?(queue)
          redis.type("queue:#{queue}") == 'zset'
        end

        def push_with_priority(queue, item, priority = :normal)
          watch_queue(queue)
          redis.zadd "queue:#{queue}", sym_to_priority(priority), encode(item)
        end

        def _pop(queue)
          case Resque.redis.type "queue:#{queue}"
            when 'zset'
              pop_priority(queue)
            else
              pop_sequential(queue)
          end
        end

        def _size(queue)
          case Resque.redis.type "queue:#{queue}"
            when 'zset'
              size_priority(queue)
            else
              size_sequential(queue)
          end

        end

        def _push(queue, item)
          case Resque.redis.type "queue:#{queue}"
            when 'zset'
              push_with_priority(queue, item)
            else
              push_sequential(queue, item)
          end
        end          

        protected

        def pop_priority(queue)
          # use zrevrange since we should order priority highest to lowest
          full_queue_name = "queue:#{queue}"
          result = redis.zrevrange(full_queue_name, 0, 0)
          job = result.nil? ? nil : result.first

          ret = decode(job)
          redis.zrem full_queue_name, job
          ret
        end

        def size_priority(queue)
          Resque.redis.zcard "queue:#{queue}"
        end

        def sym_to_priority(sym)
          if sym.is_a? Symbol
            case sym
              when :high
                100
              when :normal
                50
              when :low
                0
            end
          elsif sym.is_a? Numeric
            sym
          else
            sym.to_i rescue 0
          end
          
        end

        
      end


      module JobMethods
        def create_with_priority(queue, klass, priority, *args)

          raise NoQueueError.new("Jobs must be placed onto a queue.") if !queue

          raise NoClassError.new("Jobs must be given a class.") if klass.to_s.empty?

          ret = Resque.push_with_priority(queue, { :class => klass.to_s, :args => args }, priority)
          Plugin.after_enqueue_hooks(klass).each do |hook|
            klass.send(hook, *args)
          end
          ret
        end
      end

    end
  end
end
