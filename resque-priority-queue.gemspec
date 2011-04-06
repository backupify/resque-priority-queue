spec = Gem::Specification.new do |s|
  s.name              = 'resque-priority-queue'
  s.version           = '0.0.1'
  s.date              = Time.now.strftime('%Y-%m-%d')
  s.summary           = 'A resque plugin; gives the ability to assign priorities to individual jobs in resque'
  s.homepage          = 'http://github.com/backupify/resque-priority-queue'
  s.authors           = ['Dave Benvenuti']
  s.email             = 'dave@backupify.com'
  s.has_rdoc          = false

  s.files             = %w(Rakefile README)
  s.files            += Dir.glob('{test/*,lib/**/*}')
  s.require_paths     = ['lib']

  s.add_dependency('resque', '>= 1.8.0')
  s.add_development_dependency('test-unit', 'mocha')

  s.description       = <<-EOL
  resque-priority-queue gives the ability to assign priorities to individual jobs in resque

  Features:

  * Redis backed retry count/limit.
  * Retry on all or specific exceptions.
  * Exponential backoff (varying the delay between retrys).
  * Multiple failure backend with retry suppression & resque-web tab.
  * Small & Extendable - plenty of places to override retry logic/settings.
  EOL
end
