require 'pg'
require 'date'

# FILE IO ======================================================

def read_file(filename)
  filename = "csv/#{filename}"
  File.open(filename, 'r') do |csvfile|
    headers = csvfile.gets.chomp.split('|')
    
    linenumber = 1
    while line = csvfile.gets
      line.chomp!
      params = {}
      fields = line.split('|')
      fields.each_with_index do |field, i|
        params[headers[i]] = field
      end
      params['linenumber'] = linenumber
      linenumber += 1

      puts "PARAMS FROM FILE: #{params}"
      yield(params)

    end
  end
end

# PARSE Helpers =================================================

def dequote(s)
  return if s.nil?
  s = s.chomp.chomp('"')
  s.slice!(0) if s[0] == '"'
  s
end

def invalid_date?(y, m, d)
  return true unless (1900 .. Date.today.year) === y.to_i 
  return true unless (1 .. 12) === m.to_i
  return true unless (1 .. 31) === d.to_i  # yeah, it's not 100% reliable
  false
end

def extract_date(s, format)
  return if s.nil? || s.length < format.length
  y = s[format.index('yyyy'), 4]
  m = s[format.index('mm'), 2]
  d = s[format.index('dd'), 2]
  if (1 .. 200) === y.to_i
    y = 1900 + y.to_i
  end
  invalid_date?(y, m, d) ? nil : "#{y}-#{m}-#{d}"
end

def timestamps
  {created_at: 'now', updated_at: 'now'}
end

def prepare_params(params)
  params.transform_values! do |v| 
    v.respond_to?(:gsub) ? v.gsub("'", "\\'") : v  
  end
  params.merge! timestamps
  puts "PARAMS: #{params}"
  params
end

# DB Helpers =========================================================

def query(conn, sql, params=nil)
  puts "#{sql} [#{params}]"
  params.nil? ? 
          conn.exec(sql) :
          conn.exec_params(sql, params)
end

def find(conn, table, id)
  result = query conn, "SELECT * FROM #{table} WHERE id=#{id};"
  (result.ntuples > 0) ? result[0] : nil
end

def find_last(conn, table)
  result = query conn, "SELECT * FROM #{table} ORDER BY id DESC LIMIT 1;"
  result[0]
end

def exists?(conn, table, id)
  result = query conn, "SELECT id FROM #{table} WHERE id=#{id};"
  return result.ntuples > 0
end

def update_sequence(conn, table)
  sequence_res = conn.exec("SELECT pg_get_serial_sequence('#{table}', 'id');")
  sequence = sequence_res[0]['pg_get_serial_sequence']
  seq_val = conn.exec("SELECT last_value FROM #{sequence};")[0]['last_value']
  biggest_id = conn.exec("SELECT MAX(id) FROM #{table};")[0]['max']
  
  unless seq_val > biggest_id
    restart = biggest_id.to_i + 1
    conn.exec("ALTER SEQUENCE #{sequence} RESTART WITH #{restart};")
  end
end

def insert(conn, table, params)
  params = prepare_params params
  fields = params.keys
  sql = "INSERT INTO #{table} ("
  sql += fields.join(', ')
  sql += ") VALUES("
  sql += fields.collect{ |field| "'#{params[field]}'"}.join(', ')
  sql += ");"
  puts query(conn, sql).cmd_status
  update_sequence(conn, table) if params[:id]
end

def update(conn, table, params)
  params = prepare_params params
  fields = params.keys
  sql = "UPDATE #{table} SET "
  sql += fields.collect{ |field| "#{field} = '#{params[field]}'"}.join(', ')
  sql += " WHERE id=#{params[:id]};"
  puts query(conn, sql).cmd_status
end

def insert_or_update(conn, table, params)
  sql = "SELECT id FROM #{table} WHERE id=$1;"
  existing = query(conn, sql, [params[:id]])

  if exists?(conn, table, params[:id])
    update conn, table, params
  else
    insert conn, table, params
  end
end

# Employee Import ====================================================

def person_names(params)
  name = (params['Name']=='') ? params['ShortName'] : params['Name']
  split = name.index(' ')
  if split.nil?
    last = name
    first = ''
  else
    last = name[0, split]
    first = name[split+1 .. -1].lstrip
  end
  return first, last
end

def add_employee_person(conn, params)
  first, last = person_names params
  gender = (params['Gender']=='F') ? '1' : '0'
  birth = extract_date params['BirthDate'], 'dd/mm/yyyy'
  save_params = {id: params['EmployeeID'], first_name: first, last_name: last, gender: gender}
  save_params[:birth_date] = birth unless birth.nil?
  insert_or_update conn, 'people', save_params
end

# Child Import ====================================================

def child_names(name)
  split = name.index(' ')
  if split.nil?
    first = name
    last = ''
  else
    last = name[0, split]
    first = name[split+1 .. -1].lstrip
  end
  return first, last
end

def insert_child(conn, person_params, child_params)
  conn.transaction do |conn|
    insert conn, 'people', person_params
    person = find_last conn, 'people'
    child_params[:person_id] = person['id']
    insert conn, 'children', child_params
  end
end

def update_child(conn, child, person_params, child_params)
  update conn, 'children', child_params
  person_params[:id] = child['person_id']
  update conn, 'people', person_params
end

def add_child(conn, params)
  # Don't add children if the parent doesn't exist
  return unless exists? conn, 'people', params['EmployeeId']

  first, last = child_names params['Name']
  birth = extract_date params['BirthDate'], 'dd/mm/yyyy'
  person_params = {first_name: first, 
                   last_name: last,}
  person_params[:birth_date] = birth unless birth.nil?                   
  child_params = {id: params['linenumber'], 
                  parent_id: params['EmployeeId'], 
                  is_student: params['Student']}
  puts "CHILD PARAMS: #{child_params}"

  child = find conn, 'children', child_params[:id]
  if child.nil?
    insert_child conn, person_params, child_params
  else
    update_child conn, child, person_params, child_params
  end
end


# ============ START HERE ===================================
# ===========================================================

conn = PG.connect(dbname: 'cmbpayroll_dev',
                  host: 'localhost',
                  user: 'cmbpayroll', 
                  password: 'cmbpayroll')

read_file('employees.csv') do |params|
  add_employee_person conn, params
  puts ''
end

read_file('children.csv') do |params|
  add_child conn, params
  puts ''
end