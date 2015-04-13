/*
    MongoDB DataProvider Sitecore module
    Copyright (C) 2012  Robin Hermanussen 
    Copyright (C) 2015  Alen Pelin

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

namespace MongoDataProvider
{
  using System;
  using System.Collections.Generic;
  using System.IO;
  using System.Linq;
  using Data;
  using MongoDB.Bson;
  using MongoDB.Driver;
  using MongoDB.Driver.Builders;
  using MongoDB.Driver.GridFS;
  using Sitecore;
  using Sitecore.Caching;
  using Sitecore.Collections;
  using Sitecore.Configuration;
  using Sitecore.Data;
  using Sitecore.Data.DataProviders;
  using Sitecore.Data.Items;
  using Sitecore.Data.Managers;
  using Sitecore.Diagnostics;

  /// <summary>
  /// Provides item data from a Mongo database (www.mongodb.org).
  /// Configure this through MongoDataProvider.config.
  /// </summary>
  [UsedImplicitly]
  public class MongoDataProvider : DataProvider
  {
    /// <summary>
    /// The ID of the item that is the parent of all data in this provider.
    /// </summary>
    [NotNull]
    private readonly ID joinParentId;

    [NotNull]
    private readonly MongoDatabase database;

    [NotNull]
    private readonly MongoCollection<Data.Item> items;

    [NotNull]
    private readonly MongoGridFS gridFs;

    /// <summary>
    /// If true, MongoDB ensures that data is written to disk when inserting/updating (slower, but more reliable).
    /// </summary>
    [NotNull]
    private readonly SafeMode safeMode;

    [NotNull]
    private readonly object prefetchCacheLock = new object();

    private readonly long prefetchCacheSize = Settings.Caching.DefaultDataCacheSize;

    [CanBeNull]
    private Cache prefetchCache;

    public MongoDataProvider([NotNull] string joinParentId, [NotNull] string mongoConnectionString, [NotNull] string databaseName, [NotNull] string safeMode)
    {
      Assert.ArgumentNotNull(joinParentId, "joinParentId");
      Assert.ArgumentNotNull(mongoConnectionString, "mongoConnectionString");
      Assert.ArgumentNotNull(databaseName, "databaseName");
      Assert.ArgumentNotNull(safeMode, "safeMode");

      bool parsedSafeMode;
      var mode = SafeMode.Create(bool.TryParse(safeMode, out parsedSafeMode) && parsedSafeMode);
      Assert.IsNotNull(mode, "mode");

      var server = new MongoClient(mongoConnectionString).GetServer();
      Assert.IsNotNull(server, "server");

      var db = server.GetDatabase(databaseName);
      Assert.IsNotNull(db, "db");

      var mongoGridFs = db.GridFS;
      Assert.IsNotNull(mongoGridFs, "mongoGridFs");
      
      var collection = this.GetItemsCollection(db, mode);

      this.database = db;
      this.gridFs = mongoGridFs;
      this.joinParentId = new ID(joinParentId);
      this.safeMode = mode;
      this.items = collection;
    }

    [NotNull]
    protected Cache PrefetchCache
    {
      get
      {
        var cache = this.prefetchCache;
        if (cache != null)
        {
          return cache;
        }

        lock (this.prefetchCacheLock)
        {
          cache = this.prefetchCache;
          if (cache != null)
          {
            return cache;
          }

          const string Name = "MongoDataProvider - Prefetch data";

          cache = Cache.GetNamedInstance(Name, this.prefetchCacheSize);
          Assert.IsNotNull(cache, "cache");

          var cacheOptions = this.CacheOptions;
          if (cacheOptions != null && cacheOptions.DisableAll)
          {
            cache.Enabled = false;
          }

          this.prefetchCache = cache;

          return cache;
        }
      }
    }

    [CanBeNull]
    public override ItemDefinition GetItemDefinition([NotNull] ID itemId, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemId, "itemId");
      Assert.ArgumentNotNull(context, "context");

      PrefetchData prefetchData = this.GetPrefetchData(itemId);
      if (prefetchData == null)
      {
        return null;
      }

      return prefetchData.ItemDefinition;
    }

    [CanBeNull]
    public override VersionUriList GetItemVersions([NotNull] ItemDefinition itemDefinition, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemDefinition, "itemDefinition");
      Assert.ArgumentNotNull(context, "context");

      var id = itemDefinition.ID;
      Assert.IsNotNull(id, "id");

      var result = this.items.FindOneById(id.ToGuid());
      if (result == null)
      {
        return null;
      }

      var versions = new VersionUriList();
      var versionsList = new List<VersionUri>();
      foreach (var fieldValueId in result.FieldValues.Keys.Where(fv => fv != null && fv.Version.HasValue && fv.Language != null))
      {
        if (fieldValueId == null)
        {
          continue;
        }

        if (versionsList.Any(ver => ver != null && fieldValueId.Matches(ver)))
        {
          continue;
        }

        var version = fieldValueId.Version;
        Assert.IsNotNull(version, "version");

        var newVersionUri = new VersionUri(LanguageManager.GetLanguage(fieldValueId.Language), new Sitecore.Data.Version(version.Value));
        versionsList.Add(newVersionUri);
      }

      foreach (var version in versionsList)
      {
        versions.Add(version);
      }

      return versions;
    }

    [CanBeNull]
    public override FieldList GetItemFields([NotNull] ItemDefinition itemDefinition, [NotNull] VersionUri versionUri, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemDefinition, "itemDefinition");
      Assert.ArgumentNotNull(versionUri, "versionUri");
      Assert.ArgumentNotNull(context, "context");

      var id = itemDefinition.ID;
      Assert.IsNotNull(id, "id");

      var result = this.items.FindOneById(id.ToGuid());
      if (result == null)
      {
        return null;
      }

      var fields = new FieldList();
      foreach (var fieldValue in result.FieldValues.Where(fv => fv.Key != null && fv.Key.Matches(versionUri)))
      {
        var key = fieldValue.Key;
        Assert.IsNotNull(key, "key");

        fields.Add(new ID(key.FieldId), fieldValue.Value);
      }

      return fields;
    }

    [NotNull]
    public override IDList GetChildIDs([NotNull] ItemDefinition itemDefinition, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemDefinition, "itemDefinition");
      Assert.ArgumentNotNull(context, "context");

      var parentId = itemDefinition.ID == this.joinParentId ? Guid.Empty : itemDefinition.ID.ToGuid();
      var query = Query.EQ("ParentID", parentId);
      var cursor = this.items.FindAs<ItemBase>(query);
      Assert.IsNotNull(cursor, "cursor");

      return IDList.Build(cursor.Select(it => new ID(it.ID)).ToArray());
    }

    [CanBeNull]
    public override ID GetParentID([NotNull] ItemDefinition itemDefinition, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemDefinition, "itemDefinition");
      Assert.ArgumentNotNull(context, "context");

      var result = this.items.FindOneByIdAs<ItemBase>(itemDefinition.ID.ToGuid());
      return result != null ? (result.ParentID != Guid.Empty ? new ID(result.ParentID) : this.joinParentId) : null;
    }

    public override bool CreateItem(ID itemID, string itemName, ID templateID, ItemDefinition parent, CallContext context)
    {
      var current = this.items.FindOneByIdAs<ItemBase>(itemID.ToGuid());
      if (current != null)
      {
        // item already exists
        return false;
      }

      if (parent != null)
      {
        var parentItem = this.items.FindOneByIdAs<ItemBase>(parent.ID.ToGuid());
        if (parentItem == null)
        {
          // parent item does not exist in this provider
          return false;
        }
      }

      var itemInfo = new ItemInfo
      {
        ID = itemID.ToGuid(),
        Name = itemName,
        TemplateID = templateID.ToGuid(),
        ParentID = parent != null ? parent.ID.ToGuid() : Guid.Empty
      };

      this.items.Save(itemInfo, this.safeMode);

      return true;
    }

    public override int AddVersion([NotNull] ItemDefinition itemDefinition, [NotNull] VersionUri baseVersion, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemDefinition, "itemDefinition");
      Assert.ArgumentNotNull(baseVersion, "baseVersion");
      Assert.ArgumentNotNull(context, "context");

      Data.Item current = this.items.FindOneById(itemDefinition.ID.ToGuid());
      if (current == null)
      {
        return -1;
      }

      var num = -1;
      if (baseVersion.Version != null && baseVersion.Version.Number > 0)
      {
        // copy version
        var currentFieldValues = current.FieldValues.Where(fv => fv.Key.Matches(baseVersion)).ToList();
        var maxVersionNumber = currentFieldValues.Max(fv => fv.Key.Version);
        num = maxVersionNumber.HasValue && maxVersionNumber > 0 ? maxVersionNumber.Value + 1 : -1;

        if (num > 0)
        {
          foreach (KeyValuePair<FieldValueId, string> fieldValue in currentFieldValues)
          {
            var key = fieldValue.Key;
            Assert.IsNotNull(key, "key");

            var fieldValueId = new FieldValueId
            {
              FieldId = key.FieldId,
              Language = key.Language,
              Version = num
            };

            current.FieldValues.Add(fieldValueId, fieldValue.Value);
          }
        }
      }

      if (num == -1)
      {
        num = 1;

        // add blank version
        var fieldValueId = new FieldValueId()
        {
          FieldId = FieldIDs.Created.ToGuid(),
          Language = baseVersion.Language.Name,
          Version = num
        };

        current.FieldValues.Add(fieldValueId, string.Empty);
      }

      this.items.Save(current, this.safeMode);

      return num;
    }

    public override bool DeleteItem([NotNull] ItemDefinition itemDefinition, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemDefinition, "itemDefinition");
      Assert.ArgumentNotNull(context, "context");

      var result = this.items.Remove(Query.EQ("_id", itemDefinition.ID.ToGuid()), RemoveFlags.Single, this.safeMode);

      return result != null && result.Ok;
    }

    public override bool SaveItem([NotNull] ItemDefinition itemDefinition, [NotNull] Sitecore.Data.Items.ItemChanges changes, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemDefinition, "itemDefinition");
      Assert.ArgumentNotNull(changes, "changes");
      Assert.ArgumentNotNull(context, "context");

      var current = this.items.FindOneById(itemDefinition.ID.ToGuid());
      if (current == null)
      {
        return false;
      }

      if (changes.HasPropertiesChanged)
      {
        current.Name = StringUtil.GetString(changes.GetPropertyValue("name"), itemDefinition.Name);

        var templateId = MainUtil.GetObject(changes.GetPropertyValue("templateid"), itemDefinition.TemplateID) as ID;
        current.TemplateID = templateId != ID.Null ? templateId.ToGuid() : Guid.Empty;

        var branchId = MainUtil.GetObject(changes.GetPropertyValue("branchid"), itemDefinition.BranchId) as ID;
        current.BranchID = branchId != ID.Null ? branchId.ToGuid() : Guid.Empty;
      }

      if (!changes.HasFieldsChanged)
      {
        return true;
      }

      foreach (FieldChange change in changes.FieldChanges)
      {
        if (change == null)
        {
          continue;
        }

        var fieldVersionUri = new VersionUri(
          change.Definition == null || change.Definition.IsShared ? null : change.Language,
          change.Definition == null || change.Definition.IsUnversioned ? null : change.Version);

        var matchingFields = current.FieldValues.Where(fv => fv.Key.Matches(fieldVersionUri) && fv.Key.FieldId.Equals(change.FieldID.ToGuid()));
        Assert.IsNotNull(matchingFields, "matchingFields");
        
        if (change.RemoveField)
        {
          if (matchingFields.Any())
          {
            current.FieldValues.Remove(matchingFields.First().Key);
          }
        }
        else
        {
          if (matchingFields.Any())
          {
            current.FieldValues[matchingFields.First().Key] = change.Value;
          }
          else
          {
            var fieldValueId = new FieldValueId()
            {
              FieldId = change.FieldID.ToGuid(),
              Language = fieldVersionUri.Language != null ? fieldVersionUri.Language.Name : null,
              Version = fieldVersionUri.Version != null ? fieldVersionUri.Version.Number : null as int?
            };

            current.FieldValues.Add(fieldValueId, change.Value);
          }
        }
      }

      this.items.Save(current, this.safeMode);

      return true;
    }

    [NotNull]
    public override IdCollection GetTemplateItemIds([NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(context, "context");
      var query = Query.EQ("TemplateID", TemplateIDs.Template.ToGuid());
      var ids = new IdCollection();

      var cursor = this.items.FindAs<ItemBase>(query);
      Assert.IsNotNull(cursor, "cursor");

      foreach (var id in cursor.Select(it => new ID(it.ID)))
      {
        ids.Add(id);
      }

      return ids;
    }

    public override bool BlobStreamExists(Guid blobId, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(context, "context");

      return this.gridFs.Exists(Query.EQ("filename", new BsonString(new ShortID(blobId).ToString())));
    }

    [CanBeNull]
    public override Stream GetBlobStream(Guid blobId, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(context, "context");
      var gridFsFile = this.database.GridFS.FindOne(Query.EQ("filename", new BsonString(new ShortID(blobId).ToString())));

      return gridFsFile != null && gridFsFile.Exists ? gridFsFile.OpenRead() : null;
    }

    public override bool SetBlobStream([NotNull] System.IO.Stream stream, Guid blobId, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(stream, "stream");
      Assert.ArgumentNotNull(context, "context");

      var result = this.gridFs.Upload(stream, new ShortID(blobId).ToString());

      return result != null;
    }

    [CanBeNull]
    private PrefetchData GetPrefetchData([NotNull] ID itemId)
    {
      Assert.ArgumentNotNull(itemId, "itemId");
      var data = this.PrefetchCache[itemId] as PrefetchData;
      if (data != null)
      {
        if (!data.ItemDefinition.IsEmpty)
        {
          return data;
        }

        return null;
      }

      var result = this.items.FindOneByIdAs<ItemInfo>(itemId.ToGuid());

      if (result == null)
      {
        return null;
      }

      data = new PrefetchData(new ItemDefinition(itemId, result.Name, new ID(result.TemplateID), new ID(result.BranchID)), new ID(result.ParentID));
      this.PrefetchCache.Add(itemId, data, data.GetDataLength());

      return data;
    }

    [NotNull]
    private MongoCollection<Data.Item> GetItemsCollection([NotNull] MongoDatabase db, [NotNull] SafeMode mode)
    {
      Assert.ArgumentNotNull(db, "db");
      Assert.ArgumentNotNull(mode, "mode");

      var collection = db.GetCollection<Data.Item>("items", this.safeMode);
      Assert.IsNotNull(collection, "collection");

      collection.EnsureIndex(IndexKeys.Ascending(new[] { "ParentID" }));
      collection.EnsureIndex(IndexKeys.Ascending(new[] { "TemplateID" }));

      // ensure not empty
      if (collection.Count() > 0)
      {
        return collection;
      }

      // Create a root item and insert it
      var rootItem = new Data.Item
      {
        ID = new ID("{11111111-1111-1111-1111-111111111111}").ToGuid(),
        Name = "sitecore",
        TemplateID = new ID("{C6576836-910C-4A3D-BA03-C277DBD3B827}").ToGuid()
      };

      collection.Insert(rootItem, mode);

      return collection;
    }
  }
}
