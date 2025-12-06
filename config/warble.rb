# Disable Rake-environment-task framework detection by uncommenting/setting to false
# Warbler.framework_detection = false

# Warbler web application assembly configuration file
Warbler::Config.new do |config|
  config.features = %w(compiled runnable)
  config.dirs = %w(lib)
  config.java_libs += FileList["lib/java/*.jar"]
  config.jar_name = "tapsoob"
  config.autodeploy_dir = "dist/"
  config.jar_extension = "jar"
end
