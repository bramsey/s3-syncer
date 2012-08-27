#!/usr/bin/env ruby

require 'rubygems'
require 'yaml'
require 'aws-sdk'

def load_config
    config = YAML.load_file('config.yaml')
    @access_key_id = config['s3_info']['access_key_id']
    @secret_access_key = config['s3_info']['secret_access_key']
    @bucket = config['s3_info']['bucket']

    # set sync dir based on config.
    @sync_dir = config['s3_info']['sync_dir']
    Dir.chdir(Dir.pwd + '/' + @sync_dir)

    AWS.config(
      :access_key_id =>  @access_key_id, 
      :secret_access_key => @secret_access_key
    )
    @s3 = AWS::S3.new
    @s3_bucket = @s3.buckets[@bucket]
end

def list_files
    @s3.buckets[@bucket].objects.each do |obj|
        head = obj.head
        puts head.etag
        puts head.last_modified
        #puts "file: #{obj.key}"
        #puts "content-length: #{obj.content_length}"
        #puts "md5: #{obj.etag}"
    end
end

def upload_file(file_name)
    key = File.basename(file_name)
    @s3_bucket.objects[key].write(:file => file_name)
    puts "Uploading file #{file_name} to bucket #{@bucket}."
end

def get_file(file_name)
    key = File.basename(file_name)
    @s3_bucket.objects[key].read
    File.open(file_name, 'w') do |file|
        puts "Downloading #{file_name} from bucket #{@bucket}"
        @s3_bucket.objects[key].read do |chunk|
            file.write(chunk)
        end
    end
end

def rename_file(old_name, new_name)
    @s3_bucket.objects[old_name].move_to(new_name)
end

def delete_file(file_name)
    key = File.basename(file_name)
    @s3_bucket.objects[key].delete
    puts "Deleting file #{file_name} from bucket #{@bucket}"
end

load_config

get_file('readme.txt')
#list_files
#upload_file('billstripe/readme.txt')
