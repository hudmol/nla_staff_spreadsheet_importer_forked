class GenericDLCConverter < Converter

  def self.instance_for(type, input_file)
    if type == "generic_dlc"
      self.new(input_file)
    else
      nil
    end
  end


  def self.import_types(show_hidden = false)
    [
      {
        :name => "generic_dlc",
        :description => "Generic Digital Library Collections CSV"
      }
    ]
  end


  def self.profile
    "Convert a generic DLC CSV export to ArchivesSpace JSONModel records"
  end


  def initialize(input_file)
    super
    @batch = ASpaceImport::RecordBatch.new
    @input_file = input_file
    @records = []
    @item_count = 0
    @created_agents = {}

    @level_map = {
      'Set' => 'collection',
      'Item' => 'item'
    }

    @columns = [
                'object_id', # item row = container_indicator and processinfo_note, set row = ignore
                'file_name', # ignore
                'parent',  # ignore
                'alias', # ignore
                'parent_object_id', # ignore
                'inherit_from_parent', # ignore
                'child_order', # ignore
                'bib_level', # level, Set = Resource, Item = AO
                'collection', # ignore
                'form', # ignore
                'bib_id', # ud_int_2
                'title', # title
                'creator', # creator - strip off extra ID info etc
                'holdings_number', # Items have component_id, Set has collection_id - strip off junk
                'extent', # Set - number = number, type = items, Item - number = 1, type = items
                'internal_access_conditions', # ignore
                'availability_to_public', # ignore
                'expiry_date', # ignore
                'constraint', # ignore
                'project_reveal_availability_to_public', # ignore
                'copyright_policy', # ignore
                'sensitive_material', # ignore
                'sensitive_reason', # ignore
                'internal_comments', # ignore
                'external_comments', # scopecontent_note
                'subunit_no', # ignore
                'subunit_type', # ignore
                'issue_date', # ignore
                'start_date', # start
                'end_date', # end - may be blank
                'send_to_catalogue', # ignore
                'software', # ignore
                'device', # ignore
                'co_masters', # ignore
                'manipulation', # ignore
                'original', # ignore
                'access_only', # ignore
                'record_source', # ignore
                'digitisation_notes', # ignore
                'sheet_name', # ignore
                'sheet_creation_date', # ignore
                'additional_series_statement', # ignore
               ]
  end


  def run
    rows = CSV.read(@input_file)

    begin
      while(row = rows.shift)
        values = row_values(row)

        next if values.compact.empty?

        values_map = Hash[@columns.zip(values)]

        # NOTE: Unlike the standard DLC export, the order of collection and items
        #       is variable - ie there may be items before the collection.
        #       This requires a little post-processing to inject the resource_uri
        #       into the items, and the extent_numbder into the resource -- see below

        # NOTE: The Generic DLC export seems to always have several rows at the top
        #       that have instructions - we skip them because we only create records
        #       for rows that have 'Set' or 'Item' in the bib_level column (H)
        case format_level(values_map['bib_level'])
        when 'collection'
          @resource_uri = get_or_create_resource(values_map)
          if @resource_uri.nil?
            raise "No resource defined"
          end
        when 'item'
          add_item(values_map)
          @item_count += 1
        end
      end
    rescue StopIteration
    end

    # Now give each item it's resource_uri, and the resouce its extent number
    @records.each do | rec|
      if rec.jsonmodel_type == 'archival_object'
        rec.resource[:ref] = @resource_uri
      elsif rec.jsonmodel_type == 'resource'
        rec.extents[0][:number] = @item_count.to_s
      end
    end

    # assign all records to the batch importer in reverse
    # order to retain position from spreadsheet
    @records.reverse.each{|record| @batch << record}
  end


  def get_output_path
    output_path = @batch.get_output_path

    p "=================="
    p output_path
    p File.read(output_path)
    p "=================="

    output_path
  end


  private

  def get_or_create_resource(row)
    # sometimes holdings_number has some junk, like this:
    #     PIC Online access #PIC/21291/1-13
    # assuming the hash is reliable

    uri = "/repositories/12345/resources/import_#{SecureRandom.hex}"

    @records << JSONModel::JSONModel(:resource).from_hash({
                    :uri => uri,
                    :id_0 => row['holdings_number'].sub(/.*\#/, '').strip,
                    :title => row['title'],
                    :level => 'collection',
                    :extents => [
                                 {
                                   :portion => 'whole',
                                   :extent_type => 'items',
                                   :container_summary => row['extent'],
                                   :number => 'BLATME',
                                 }
                                ],
                    :dates => [format_date(row['start_date'], row['end_date'])].compact,
                    :linked_agents => [format_agent(row)].compact,
                    :user_defined => format_user_defined(row),
                    :finding_aid_language => 'eng',
                    :finding_aid_script => 'Latn',
                    :lang_materials => [
                                        {
                                          :language_and_script => {
                                            :language => 'eng',
                                            :script => 'Latn'
                                          }
                                        }
                                       ],
                  })

      uri
  end


  def create_digital_object(row)
    uri = "/repositories/12345/digital_objects/import_#{SecureRandom.hex}"

    do_hash = {
      :uri => uri,
      :digital_object_id => row['object_id'],
      :title => row['title'],
      :linked_agents => [format_agent(row)].compact,
      :user_defined => format_user_defined(row)
    }

    @records << JSONModel::JSONModel(:digital_object).from_hash(do_hash)

    uri
  end


  def get_or_create_agent(primary_name)
    if (uri = @created_agents[primary_name])
      uri
    elsif (name = NamePerson[:primary_name => primary_name])
      AgentPerson[name.agent_person_id].uri
    else
      uri = "/agents/people/import_#{SecureRandom.hex}"

      agent_hash = {
        :uri => uri,
        :names => [
                   {
                     :primary_name => primary_name,
                     :sort_name_auto_generate => true,
                     :name_order => 'inverted',
                     :source => 'local'
                   }
                  ]
      }

      @records << JSONModel::JSONModel(:agent_person).from_hash(agent_hash)
      @created_agents[primary_name] = uri

      uri
    end
  end


  def add_item(row)
    @records << JSONModel::JSONModel(:archival_object).from_hash(format_ao(row))
  end


  def format_level(level_string)
    @level_map[level_string]
  end


  def format_date(start_date, end_date)
    return if start_date.nil?

    # reformat if necessary
    start_date.sub!(/(\d\d)\/(\d\d)\/(\d\d\d\d)/, '\3-\2-\1')
    end_date.sub!(/(\d\d)\/(\d\d)\/(\d\d\d\d)/, '\3-\2-\1') if end_date

    {
      :date_type => end_date ? 'inclusive' : 'single',
      :label => 'creation',
      :begin => start_date,
      :end => end_date,
      :expression => [start_date, end_date].compact.join('-')
    }
  end


  def format_instance(row)
    {
      :instance_type => 'digital_object',
      :digital_object => {
        :ref => create_digital_object(row)
      }
    }
  end


  def format_agent(row)
    return unless row['creator']

    # sometimes the creator is suffixed with some long id-like info, eg:
    #   Henningham, Leigh, 1960- photographer. 900747 4edef81f-a657-5334-b6ea-656183c3f08d
    #                                          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    # assuming it will have this shape if present

    {
      :role => 'creator',
      :ref => get_or_create_agent(row['creator'].sub(/ +\h{8}(-\h{4}){3}-\h{12}/, '').sub(/ +\d{6}/, ''))
    }
  end


  def format_user_defined(row)
    if row['bib_id']
      {
        :integer_2 => row['bib_id']
      }
    end
  end

  def row_values(row)
    (0...row.size).map {|i| row[i] ? row[i].to_s.strip : nil}
  end


  def format_ao(row)
    # NOTE: The resource ref below will be overwritten in postprocessing.
    hash = {
      :uri => "/repositories/12345/archival_objects/import_#{SecureRandom.hex}",
      :title => row['title'],
      :component_id => row['holdings_number'].sub(/.*\#/, '').strip,
      :level => format_level(row['bib_level']),
      :dates => [format_date(row['start_date'], row['end_date'])].compact,
      :extents => [
                   {
                     :portion => 'part',
                     :extent_type => 'items',
                     :container_summary => row['extent'],
                     :number => '1',
                   }
                  ],
      :instances => [format_instance(row)].compact,
      :notes => [],
      :linked_agents => [format_agent(row)].compact,
      :resource => {
        :ref => "/repositories/12345/resources/import_#{SecureRandom.hex}",
      },
    }

    if row['external_comments']
      hash[:notes] << {
        :jsonmodel_type => 'note_multipart',
        :type => 'scopecontent',
        :subnotes =>[{
                       :jsonmodel_type => 'note_text',
                       :content => row['external_comments']
                     }]
      }
    end
    if row['object_id']
      hash[:notes] << {
        :jsonmodel_type => 'note_multipart',
        :type => 'processinfo',
        :subnotes =>[{
                       :jsonmodel_type => 'note_text',
                       :content => row['object_id']
                     }]
      }
    end

    hash
  end
end
