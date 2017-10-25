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

      # puts "PARAMS FROM FILE: #{params}"
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

def timestamps(existing)
  existing ? {updated_at: 'now'} : 
             {created_at: 'now', updated_at: 'now'}
end

def prepare_params(params)
  params.transform_values! do |v| 
    v.respond_to?(:gsub) ? v.gsub("'", "\\'") : v  
  end
  # puts "PARAMS: #{params}"
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
  params = prepare_params(params).merge(timestamps(false))
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
  params = prepare_params(params).merge(timestamps(true))
  fields = params.keys
  sql = "UPDATE #{table} SET "
  sql += fields.collect{ |field| "#{field} = '#{params[field]}'"}.join(', ')
  sql += " WHERE id=#{params[:id]};"
  puts query(conn, sql).cmd_status
end

def insert_or_update(conn, table, params)
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

def convert_category(c)
  c.to_i - 1
end

def convert_echelon(e)
  %w[1 2 3 4 5 6 7 8 9 10 11 12 13 A B C D E F G].index(e) 
end

def convert_wage_scale(w)
  %w[A B C D E].index(w)
end

def convert_wage_period(w)
  %w[H M].index(w)
end

def convert_employment_status(s)
  %w[F P T L Y I].index(s)
end

def convert_marital_status(s)
  %w[C M W].index(s)
end

def employee_params(params)
  # title, cnps, dipe, contract_start/end, category, echelon,
  # wage_scale, wage_period, last_raise_date, taxable_percentage,
  # transporation, employment_status, marital_status, hours_day,
  # days_week, wage, supervisor_id, department_id, amical, uniondues
  # first_day
  p = {}
  p[:id] = params['EmployeeID']
  p[:person_id] = params['EmployeeID']
  p[:title] = params['JobTitle']
  p[:cnps] = params['CNPSno']
  p[:dipe] = params['DIPESLine']
  p[:contract_start] = extract_date params['BeginContract'], 'dd/mm/yyyy'
  p[:first_day] = extract_date params['BeginContract'], 'dd/mm/yyyy'
  p[:contract_end] = extract_date params['EndContract'], 'dd/mm/yyyy'
  p[:category] = convert_category params['Category']
  p[:echelon] = convert_echelon params['Echelon']
  p[:wage_scale] = convert_wage_scale params['WageScale']
  p[:wage_period] = convert_wage_period params['CFAper']
  p[:last_raise_date] = extract_date params['LastVacation'], 'dd/mm/yyyy'
  p[:taxable_percentage] = params['TaxablePercent']
  p[:transportation] = params['Transportation']
  p[:employment_status] = convert_employment_status params['Status']
  p[:marital_status] = convert_marital_status params['MaritalStatus']
  p[:hours_day] = params['HoursPerDay'].to_i
  p[:days_week] = params['DaysPerWeek']
  p[:wage] = params['Wage']
  p[:supervisor_id] = params['SupervisorId']
  p[:department_id] = params['DepartmentId']
  p[:amical] = params['AMICAL']
  p[:uniondues] = params['Union']
  p.keys.each do |key|
    if p[key].nil? or p[key] == ''
      p.delete key
    end
  end
  # puts p
  p
end


def add_employee(conn, params)
  emp_params = employee_params params
  unless exists?(conn, 'supervisors', emp_params[:supervisor_id])
    emp_params.delete(:supervisor_id)
  end
  unless exists?(conn, 'departments', emp_params[:department_id])
    emp_params.delete(:department_id)
  end

  insert_or_update conn, 'employees', emp_params
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
  #puts "CHILD PARAMS: #{child_params}"

  child = find conn, 'children', child_params[:id]
  if child.nil?
    insert_child conn, person_params, child_params
  else
    update_child conn, child, person_params, child_params
  end
end

def add_deparment(conn, params)
  dpt_params = {id: params['DepartmentId'],
                name: params['Name'],
                description: params['Description'],
                account: params['Account']}
  insert_or_update conn, 'departments', dpt_params
end

def find_matching_person(conn, full_name)
  full_name.split(/[\. ]/).each do |name|
    if name.length > 1
      sql = "SELECT id, first_name, last_name FROM people 
             WHERE first_name ILIKE '%#{name}%' OR
                   last_name  ILIKE '%#{name}%';"
      result = query conn, sql
      result.each do |row|
        puts "Is #{full_name} the same as #{row['first_name']} #{row['last_name']}?"
        if gets[0].downcase == 'y'
          return row['id']
        end
      end
    end
  end
  return nil
end

def supervisor_names(name)
  split = name.rindex(/[\. ]/)
  if split.nil?
    return '', name
  end
  first = name.slice(0, split)
  last = name.slice (split+1 .. -1)
  return first, last
end

def add_supervisor(conn, params)
  unless exists? conn, 'supervisors', params['SupervisorId']
    person_id = find_matching_person(conn, params['Name'])
    if person_id.nil?
      first, last = supervisor_names params['Name']
      conn.transaction do |conn|
        insert conn, 'people', {first_name: first, last_name: last}
        person_id = find_last(conn, 'people')['id']
        insert conn, 'supervisors', {id: params['SupervisorId'], person_id: person_id}
      end
    else
      insert conn, 'supervisors', {id: params['SupervisorId'], person_id: person_id}
    end
  end
end

# ============ START HERE ===================================
# ===========================================================

conn = PG.connect(dbname: 'cmbpayroll_dev',
                  host: 'localhost',
                  user: 'cmbpayroll', 
                  password: 'cmbpayroll')

# read_file('departments.csv') do |params|
#   add_deparment conn, params
#   puts ''
# end

# read_file('supervisors.csv') do |params|
#   add_supervisor conn, params
#   puts ''
# end


read_file('employees.csv') do |params|
  add_employee_person conn, params
  add_employee conn, params
  puts ''
end

# read_file('children.csv') do |params|
#   add_child conn, params
#   puts ''
# end

# After we get employees assigned to supervisors
# Merge all duplicate supervisors
# Change existing test in add_supervisor to check if the id is greater
#  than the current last id.