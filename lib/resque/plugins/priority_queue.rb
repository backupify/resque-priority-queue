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

          alias_method :peek_sequential, :peek
          alias_method :peek, :_peek

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
          redis.zadd "queue:#{queue}", clean_priority(priority), encode(item)
        end

        def priority(queue, job_class, *args)
          score = redis.zscore "queue:#{queue}", encode(:class => job_class.to_s, :args => args)

          score = 1000 - score.to_i unless score.nil?

          score
        end

        def priority_enabled?(queue)
          redis.sismember 'priority_queues', queue.to_s
        end

        def _pop(queue)
          if is_priority_queue?(queue)
            pop_priority(queue)
          else
            pop_sequential(queue)
          end
        end

        def _size(queue)
          if is_priority_queue?(queue)
            size_priority(queue)
          else
            size_sequential(queue)
          end
        end

        def _push(queue, item)
          if is_priority_queue?(queue)
            push_with_priority(queue, item)
          else
            push_sequential(queue, item)
          end
        end

        def _peek(queue, start=0, count=1)
          if is_priority_queue?(queue)
            peek_priority(queue, start, count)
          else
            peek_sequential(queue, start, count)
          end
        end

      # the priority value has to be a number between 0 and 1000
        # for the queue to work right, the lower the number actually has to map to the higher priority, so
        # we return 1000 minus the priority.  here we also convert certain symbols to numeric values
        def clean_priority(sym)

          cleaned_priority = if sym.is_a? Symbol
            case sym
              when :highest
                1000
              when :high
                750
              when :normal
                500
              when :low
                250
              when :lowest
                0
              else
                0
            end
          else
            # make it an integer between 0 and 1000
            [[sym.to_i, 1000].min, 0].max rescue 0
          end

          1000 - cleaned_priority

        end

        protected

        def pop_priority(queue)
          full_queue_name = "queue:#{queue}"
          result = redis.zrange(full_queue_name, 0, 0)
          job = result.nil? ? nil : result.first

          ret = decode(job)
          Resque.redis.zrem full_queue_name, job
          ret
        end

        def size_priority(queue)
          Resque.redis.zcard "queue:#{queue}"
        end

        def peek_priority(queue, start=0, count=1)
          ret = Resque.redis.zrange("queue:#{queue}", start, start+count-1).map{ |job| decode(job) }

          if count == 1 && !ret.nil?
            ret.first
          else
            ret
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

        # just calls create_with_priority, since zadd just does an update if the job already exists
        alias_method :create_or_update_priority, :create_with_priority

      end

    end
  end
end
