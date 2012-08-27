#!/usr/bin/env ruby

require 'rubygems'
require 'yaml'
require 'aws-sdk'

def load_config
    config = YAML.load_file('config.yaml')
    @access_key_id = config['s3_info']['access_key_id']
    @secret_access_key = config['s3_info']['secret_access_key']
    @bucket = config['s3_info']['bucket']
    AWS.config(
      :access_key_id =>  @access_key_id, 
      :secret_access_key => @secret_access_key
    )
    @s3 = AWS::S3.new
end

def upload_file(file_name)
    key = File.basename(file_name)
    @s3.buckets[@bucket].objects[key].write(:file => file_name)
    puts "Uploading file #{file_name} to bucket #{@bucket}."
end

load_config

upload_file('billstripe/readme.txt')
