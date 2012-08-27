require 'yaml'
require 'net/http'

def load_config
    config = YAML.load_file('config.yaml')
    @access_key_id = config['s3_info']['access_key_id']
    @secret_access_key = config['s3_info']['secret_access_key']
    @bucket = config['s3_info']['bucket']
    puts @access_key_id
    puts @secret_access_key
    puts @bucket
end

load_config
