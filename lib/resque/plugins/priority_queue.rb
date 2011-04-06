module Resque
  module Plugins
    module PriorityQueue

      # the score is stored as the priority * multiplier + time.now.to_i, so that "ties" are handled correctly
      PRIORITY_MULTIPLIER = (1e13).to_i

      MIN_PRIORITY = 0
      MAX_PRIORITY = 1000

      def priority=(priority)
        @priority=priority
      end

      def priority
        @priority
      end


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

        Resque::Job.send(:extend, JobClassMethods)
        Resque::Job.send(:include, JobInstanceMethods)

        old_after_fork = Resque.after_fork
        Resque.after_fork do |job|
          job.payload_class.priority = job.priority if job.payload_class.respond_to?(:priority=)

          old_after_fork.call(job) if old_after_fork
        end

        @priority_queue_enabled = true
      end

      module ResqueMethods

        def is_priority_queue?(queue)
          redis.type("queue:#{queue}") == 'zset'
        end

        def push_with_priority(queue, item, priority = :normal)
          watch_queue(queue)
          redis.zadd "queue:#{queue}", calculate_job_score(priority), encode(item)
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

          cleaned_priority = case sym
            when :highest, 'highest'
              MAX_PRIORITY
            when :high, 'high'
              750
            when :normal, 'normal'
              500
            when :low, 'low'
              250
            when :lowest, 'lowest'
              MIN_PRIORITY
            else
              [[sym.to_i, 1000].min, 0].max rescue 0
          end

          MAX_PRIORITY - cleaned_priority

        end

        # given a job score (from the zset), returns { :priority => cleaned priority, :created_at => unix timestamp }
        def job_score_parts(score)
          { :priority => (score.to_i / PRIORITY_MULTIPLIER), :created_at => (score.to_i % PRIORITY_MULTIPLIER) }
        end

        protected

        def pop_priority(queue)
          full_queue_name = "queue:#{queue}"
          result = redis.zrange(full_queue_name, 0, 0)
          job = result.nil? ? nil : result.first

          job_info = job_score_parts(redis.zscore(full_queue_name, job))

          ret = decode(job)
          Resque.redis.zrem full_queue_name, job
          ret.merge('priority' => job_info[:priority], 'created_at' => job_info[:created_at])
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

        # given a priority, calculate the final score to be used when adding the job to the queue zset
        def calculate_job_score(priority)
          (clean_priority(priority) * PRIORITY_MULTIPLIER) + Time.now.to_i
        end
        
      end


      module JobClassMethods
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

      module JobInstanceMethods

        def priority
          Resque.clean_priority(@payload['priority']) if @payload
        end

      end

    end
  end
end
